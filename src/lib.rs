use std::{
    collections::{HashMap, VecDeque},
    fmt::{Display, Formatter},
    time::Duration,
};

use async_trait::async_trait;
use behaviour::{
    base::{BaseBehaviour, BaseBehaviourEvent},
    wrapped::Wrapped,
};
use builder::P2PTransportBuilder;
use cli::TransportArgs;
use futures::StreamExt;
use libp2p::{
    bytes::Bytes,
    noise,
    swarm::{DialError, SwarmEvent},
    Swarm, TransportError,
};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    time::{interval, Interval},
};

use crate::protocol::Protocol;

pub mod behaviour;
pub mod builder;
pub mod cli;
pub mod protocol;
pub mod utils;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Libp2p transport creation failed: {0}")]
    Transport(String),
    #[error("Listening failed: {0:?}")]
    Listen(#[from] TransportError<std::io::Error>),
    #[error("Dialing failed: {0:?}")]
    Dial(#[from] DialError),
    #[error("Decoding message failed: {0}")]
    Decode(String),
}

impl From<noise::Error> for Error {
    fn from(e: noise::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AgentInfo {
    pub name: &'static str,
    pub version: &'static str,
}

impl Display for AgentInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

#[macro_export]
macro_rules! get_agent_info {
    () => {
        AgentInfo {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
        }
    };
}

#[async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    /// Defines how to handle an incoming message.
    async fn dispatch(&self, message: Bytes) -> Result<(), Box<dyn core::error::Error>>;
}

#[async_trait]
pub trait TopicHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message from a topic.
    async fn dispatch(
        &self,
        topic: &str,
        message: Bytes,
    ) -> Result<(), Box<dyn core::error::Error>>;

    fn topics(&self) -> Vec<&'static str>;
}

/// The p2p swarm
pub type NodeSwarm = Swarm<Wrapped<BaseBehaviour>>;

pub type Publisher = Sender<(&'static str, Bytes)>;

/// Convenient alias for cancel handlers returned to the caller task.
pub type CancelHandler = oneshot::Receiver<Bytes>;

pub struct P2PTransport<Handler: TopicHandler> {
    /// The swarm on top of which the node operates
    swarm: NodeSwarm,
    /// Reading dispatcher for incoming topic messages
    handler: Handler,
    /// Received messages for publishing them to a topic
    rx_publisher: Receiver<(&'static str, Bytes)>,
    /// Known topics
    topics: HashMap<String, Vec<Bytes>>,
    /// Queue for messages that failed to publish
    retry_queue: VecDeque<(String, Bytes)>,
    /// Retry interval for failed messages
    retry_interval: Interval,
}

impl<Handler: TopicHandler> P2PTransport<Handler> {
    pub fn spawn<P: Protocol>(
        args: TransportArgs,
        handler: Handler,
        protocol: P,
        rx_publisher: Receiver<(&'static str, Bytes)>,
        retry_interval: Duration,
    ) {
        let agent_info = get_agent_info!();
        let builder = P2PTransportBuilder::from_cli(args, protocol, agent_info)
            .expect("Invalid transport arguments");
        // Create a default Swarm.
        let swarm = builder
            .build_default_swarm()
            .expect("Cannot build swarm for invalid arguments");

        tokio::spawn(async move {
            Self {
                swarm,
                handler,
                rx_publisher,
                topics: HashMap::new(),
                retry_queue: VecDeque::new(),
                retry_interval: interval(retry_interval),
            }
            .run()
            .await;
        });
    }

    pub async fn run(&mut self) {
        // Subscribe to all topics managed by the handler
        for topic in self.handler.topics() {
            self.swarm.behaviour_mut().subscribe(topic);
            self.topics.insert(topic.to_string(), Vec::new());
        }

        loop {
            tokio::select! {
                // Publish message to given topic
                Some((topic, data)) = self.rx_publisher.recv() => {
                    match self.swarm.behaviour_mut().publish_message(topic, data.clone()) {
                        Ok(_) => debug!("Message published to {}", topic),
                        Err(e) => {
                            warn!("Cannot publish to {}: {:?}. Queuing for retry.", topic, e);
                            self.retry_queue.push_back((topic.to_string(), data));
                        }
                    }
                },

                // Retry failed messages periodically
                _ = self.retry_interval.tick() => {
                    let mut remaining = VecDeque::new();
                    while let Some((topic, data)) = self.retry_queue.pop_front() {
                        match self.swarm.behaviour_mut().publish_message(&topic, data.clone()) {
                            Ok(_) => debug!("Retried and published message to {}", topic),
                            Err(e) => {
                                warn!("Retry failed for {}: {:?}", topic, e);
                                remaining.push_back((topic, data));
                            }
                        }
                    }
                    // Put back any messages that still failed
                    self.retry_queue = remaining;
                },

                // Handle swarm events
                event = self.swarm.select_next_some() => {
                    match event {
                        SwarmEvent::Behaviour(BaseBehaviourEvent::Gossipsub(msg)) => {
                            // Incoming published message
                            let topic = msg.topic;
                            if let Err(e) = self.handler.dispatch(topic, msg.message.into()).await {
                                warn!("Dispatch error: {}", e);
                            }
                        },

                        other => {
                            debug!("Other swarm event: {:?}", other);
                        }
                    }
                }
            }
        }
    }
}
