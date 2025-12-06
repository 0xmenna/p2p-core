use super::wrapped::{BehaviourWrapper, TToSwarm};
use futures::stream::Stream;
use futures::StreamExt;
use libp2p::PeerId;
use libp2p::{
    allow_block_list::{self, AllowedPeers},
    swarm::ToSwarm,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufRead, BufReader},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::wrappers::IntervalStream;

/// A set of whitelisted peer nodes.
pub type WhitelistedPeers = HashSet<PeerId>;

/// A trait for providing a stream of whitelisted network nodes.
pub trait WhitelistProvider {
    fn network_nodes_stream(self, interval: Duration) -> WhitelistedStream;
}

/// An error that can occur while fetching whitelisted nodes.
#[derive(Debug)]
pub struct StreamError(pub String);

impl From<io::Error> for StreamError {
    fn from(err: io::Error) -> Self {
        StreamError(err.to_string())
    }
}

/// A stream of whitelisted nodes.
pub type WhitelistedStream =
    Pin<Box<dyn Stream<Item = Result<WhitelistedPeers, StreamError>> + Send + 'static>>;

/// A default implementation of `WhitelistProvider` that reads fixed nodes from a file.
pub struct DefaultWhitelistProvider {
    file_path: PathBuf,
}

impl DefaultWhitelistProvider {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }
}

impl WhitelistProvider for DefaultWhitelistProvider {
    /// Provides a stream that reads the whitelisted nodes from the specified file at regular intervals.
    fn network_nodes_stream(self, interval: Duration) -> WhitelistedStream {
        let stream = IntervalStream::new(tokio::time::interval(interval)).then(move |_| {
            let path = self.file_path.clone();
            async move {
                let file = File::open(&path).map_err(StreamError::from)?;
                let reader = BufReader::new(file);
                let mut peers = HashSet::new();

                for line in reader.lines() {
                    let line = line?;
                    if let Ok(peer_id) = line.parse::<PeerId>() {
                        peers.insert(peer_id);
                    }
                }

                Ok(peers)
            }
        });

        Box::pin(stream)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WhitelistConfig {
    pub nodes_update_interval: Duration,
}

impl WhitelistConfig {
    pub fn new(nodes_update_interval: Duration) -> Self {
        Self {
            nodes_update_interval,
        }
    }
}

impl Default for WhitelistConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

pub struct WhitelistBehavior {
    allow: allow_block_list::Behaviour<AllowedPeers>,
    active_nodes_stream: WhitelistedStream,
    registered_nodes: HashSet<PeerId>,
}

impl WhitelistBehavior {
    pub fn new<T: WhitelistProvider>(provider: T, config: WhitelistConfig) -> Self {
        let active_nodes_stream = provider.network_nodes_stream(config.nodes_update_interval);
        Self {
            allow: Default::default(),
            active_nodes_stream,
            registered_nodes: Default::default(),
        }
    }

    pub fn allow_peer(&mut self, peer_id: PeerId) {
        log::debug!("Allowing peer {peer_id}");
        self.allow.allow_peer(peer_id);
    }

    pub fn disallow_peer(&mut self, peer_id: PeerId) {
        log::debug!("Disallowing peer {peer_id}");
        self.allow.disallow_peer(peer_id);
    }

    fn on_nodes_update(
        &mut self,
        result: Result<WhitelistedPeers, StreamError>,
    ) -> Option<WhitelistedPeers> {
        let nodes = result
            .map_err(|e| {
                log::warn!("Error retrieving registered nodes from chain: {e:?}");
                e
            })
            .ok()?;

        if nodes == self.registered_nodes {
            log::debug!("Registered nodes set unchanged.");
            return None;
        }
        log::debug!("Updating registered nodes");

        // Disallow nodes which are no longer registered
        for peer_id in self.registered_nodes.difference(&nodes) {
            log::debug!("Blocking peer {peer_id}");
            self.allow.disallow_peer(*peer_id);
        }

        // Allow newly registered nodes
        for peer_id in nodes.difference(&self.registered_nodes) {
            log::debug!("Allowing peer {peer_id}");
            self.allow.allow_peer(*peer_id);
        }
        self.registered_nodes = nodes.clone();

        Some(nodes)
    }
}

impl BehaviourWrapper for WhitelistBehavior {
    type Inner = allow_block_list::Behaviour<AllowedPeers>;
    type Event = WhitelistedPeers;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.allow
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<impl IntoIterator<Item = TToSwarm<Self>>> {
        match self.active_nodes_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => {
                Poll::Ready(self.on_nodes_update(res).map(ToSwarm::GenerateEvent))
            }
            Poll::Pending => Poll::Pending,
            _ => unreachable!(), // infinite stream
        }
    }
}
