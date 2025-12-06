use super::super::{cli::BootNode, utils::parse_env_var, AgentInfo};
use super::{
    addr_cache::AddressCache,
    pubsub::{MsgValidationConfig, PubSubError, PubsubBehaviour, PubsubMsg},
    wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
};
use crate::protocol::Protocol;
use crate::utils::addr_is_reachable;
use bimap::BiHashMap;
use futures_bounded::{Delay, FuturesMap};
use libp2p::{
    autonat::{self, NatStatus},
    bytes::Bytes,
    core::ConnectedPoint,
    dcutr, identify,
    identity::Keypair,
    kad::{
        self, store::MemoryStore, GetClosestPeersError, GetClosestPeersOk, ProgressStep, QueryId,
        QueryResult,
    },
    ping, relay,
    swarm::{
        behaviour::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        ConnectionClosed, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, ToSwarm,
    },
    Multiaddr, PeerId,
};
use libp2p_swarm_derive::NetworkBehaviour;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    task::{Context, Poll},
    time::Duration,
    vec,
};

#[cfg(feature = "whitelist")]
use {
    super::pubsub::ValidationError,
    super::whitelist::{WhitelistBehavior, WhitelistConfig},
    crate::behaviour::whitelist::WhitelistedPeers,
    parking_lot::RwLock,
    std::{collections::HashSet, sync::Arc},
};

#[derive(NetworkBehaviour)]
pub struct InnerBehaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
    relay: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    ping: ping::Behaviour,
    autonat: autonat::Behaviour,
    pubsub: Wrapped<PubsubBehaviour>,
    address_cache: AddressCache,

    #[cfg(feature = "whitelist")]
    whitelist: Wrapped<WhitelistBehavior>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BaseConfig {
    /// How often to check for on-chain updates
    pub onchain_update_interval: Duration,
    /// Timeout for autoNAT probes (default: 60 sec).
    pub autonat_timeout: Duration,
    /// How often to publish identify info to connected nodes (default: 60 sec).
    pub identify_interval: Duration,
    /// Timeout for outgoing reachability probes (default: 20 sec).
    pub probe_timeout: Duration,
    /// Timeout for kademlia DHT queries (default: 10 sec).
    pub kad_query_timeout: Duration,
    /// Maximum number of concurrent outgoing reachability probes (default: 1024)
    pub max_concurrent_probes: usize,
    /// Maximum size of gossipsub messages in bytes (default: `MAX_PUBSUB_MSG_SIZE`)
    pub max_pubsub_msg_size: usize,
    /// Maximum number of peers to keep in the address cache (default: 1024)
    pub addr_cache_size: NonZeroUsize,
    /// Minimum interval between messages from the same origin
    pub msg_interval: Duration,
}

impl BaseConfig {
    pub fn from_env() -> Self {
        let onchain_update_interval =
            Duration::from_secs(parse_env_var("ONCHAIN_UPDATE_INTERVAL_SEC", 60));
        let autonat_timeout = Duration::from_secs(parse_env_var("AUTONAT_TIMEOUT_SEC", 60));
        let identify_interval = Duration::from_secs(parse_env_var("IDENTIFY_INTERVAL_SEC", 60));
        let probe_timeout = Duration::from_secs(parse_env_var("PROBE_TIMEOUT_SEC", 20));
        let kad_query_timeout = Duration::from_secs(parse_env_var("KAD_QUERY_TIMEOUT_SEC", 5));
        let max_concurrent_probes = parse_env_var("MAX_CONCURRENT_PROBES", 1024);
        let max_pubsub_msg_size = parse_env_var("MAX_PUBSUB_MSG_SIZE", 65536);
        let addr_cache_size = NonZeroUsize::new(parse_env_var("ADDR_CACHE_SIZE", 1024))
            .expect("addr_cache_size should be > 0");
        let msg_interval = Duration::from_millis(parse_env_var("MSG_INTERVAL_MILLI", 50));
        Self {
            onchain_update_interval,
            autonat_timeout,
            identify_interval,
            probe_timeout,
            kad_query_timeout,
            max_concurrent_probes,
            max_pubsub_msg_size,
            addr_cache_size,
            msg_interval,
        }
    }
}

pub struct BaseBehaviour {
    inner: InnerBehaviour,
    keypair: Keypair,
    msg_interval: Duration,
    pending_events: VecDeque<TToSwarm<Self>>,
    pending_outbound_conns: BiHashMap<PeerId, ConnectionId>,
    ongoing_queries: BiHashMap<PeerId, QueryId>,
    outbound_conns: HashMap<PeerId, u32>,
    probe_timeouts: FuturesMap<PeerId, ()>,

    #[cfg(feature = "whitelist")]
    registered_nodes: Arc<RwLock<HashSet<PeerId>>>,
}

#[allow(dead_code)]
impl BaseBehaviour {
    pub fn new<P: Protocol>(
        keypair: &Keypair,
        config: BaseConfig,
        boot_nodes: Vec<BootNode>,
        relay: relay::client::Behaviour,
        protocol: P,
        agent_info: AgentInfo,
    ) -> Self {
        let local_peer_id = keypair.public().to_peer_id();
        log::debug!("Local peer id: {local_peer_id}");
        let mut kad_config = kad::Config::new(protocol.dht_protocol());
        kad_config.set_query_timeout(config.kad_query_timeout);
        let mut inner = InnerBehaviour {
            identify: identify::Behaviour::new(
                identify::Config::new(protocol.id(), keypair.public())
                    .with_interval(config.identify_interval)
                    .with_push_listen_addr_updates(true)
                    .with_agent_version(agent_info.to_string()),
            ),
            kademlia: kad::Behaviour::with_config(
                local_peer_id,
                MemoryStore::new(local_peer_id),
                kad_config,
            ),
            relay,
            dcutr: dcutr::Behaviour::new(local_peer_id),
            ping: ping::Behaviour::new(ping::Config::default()),
            autonat: autonat::Behaviour::new(
                local_peer_id,
                autonat::Config {
                    timeout: config.autonat_timeout,
                    ..Default::default()
                },
            ),
            pubsub: PubsubBehaviour::new(keypair.clone(), config.max_pubsub_msg_size).into(),
            address_cache: AddressCache::new(config.addr_cache_size),

            #[cfg(feature = "whitelist")]
            whitelist: WhitelistBehavior::new(
                protocol.whitelistst_provider(),
                WhitelistConfig::new(config.onchain_update_interval),
            )
            .into(),
        };

        for boot_node in boot_nodes {
            #[cfg(feature = "whitelist")]
            inner.whitelist.allow_peer(boot_node.peer_id);

            inner
                .autonat
                .add_server(boot_node.peer_id, Some(boot_node.address));
        }

        Self {
            inner,
            keypair: keypair.clone(),
            msg_interval: config.msg_interval,
            pending_events: Default::default(),
            pending_outbound_conns: Default::default(),
            ongoing_queries: Default::default(),
            outbound_conns: Default::default(),
            probe_timeouts: FuturesMap::new(
                move || Delay::tokio(config.probe_timeout),
                config.max_concurrent_probes,
            ),

            #[cfg(feature = "whitelist")]
            registered_nodes: Arc::new(RwLock::new(Default::default())),
        }
    }

    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    pub fn subscribe(&mut self, topic: &'static str) {
        #[cfg(feature = "whitelist")]
        let registered_nodes = self.registered_nodes.clone();

        let config = MsgValidationConfig::new(self.msg_interval).max_burst(2);

        #[cfg(feature = "whitelist")]
        let config = config.msg_validator(move |peer_id: PeerId, _seq_no: u64, _data: &[u8]| {
            if !registered_nodes.read().contains(&peer_id) {
                return Err(ValidationError::Invalid("Node not registered"));
            }

            Ok(())
        });

        self.inner.pubsub.subscribe(topic, config);
    }

    pub fn publish_message(&mut self, topic: &str, msg: Bytes) -> Result<(), PubSubError> {
        self.inner.pubsub.publish(topic, msg)
    }

    pub fn find_and_dial(&mut self, peer_id: PeerId) {
        if self.ongoing_queries.contains_left(&peer_id) {
            log::debug!("Query for peer {peer_id} already ongoing");
        } else {
            log::debug!("Starting query for peer {peer_id}");
            let query_id = self.inner.kademlia.get_closest_peers(peer_id);
            self.ongoing_queries.insert(peer_id, query_id);
        }
    }

    pub fn outbound_conn_exists(&self, peer_id: &PeerId) -> bool {
        self.outbound_conns.get(peer_id).is_some_and(|x| *x > 0)
    }

    fn try_schedule_probe(&mut self, peer_id: PeerId) -> Result<(), TryProbeError> {
        if self.probe_timeouts.contains(peer_id) {
            log::debug!("Probe for peer {peer_id} already ongoing");
            return Err(TryProbeError::Ongoing);
        }
        if self
            .probe_timeouts
            .try_push(peer_id, futures::future::pending())
            .is_err()
        {
            log::debug!("Too  many ongoing probes");
            return Err(TryProbeError::TooManyProbes);
        }
        if self.outbound_conn_exists(&peer_id) {
            log::debug!("Closing outbound connection(s) to {peer_id}");
            self.pending_events.push_back(ToSwarm::CloseConnection {
                peer_id,
                connection: Default::default(),
            });
        }
        log::debug!("Probing peer {peer_id}");

        Ok(())
    }

    /// Try to find peer on DHT and connect
    pub fn try_probe_dht(&mut self, peer_id: PeerId) -> Result<(), TryProbeError> {
        self.try_schedule_probe(peer_id)?;
        self.find_and_dial(peer_id);
        Ok(())
    }

    /// Try to connect to peer directly
    pub fn try_probe_direct(
        &mut self,
        peer_id: PeerId,
        addr: Multiaddr,
    ) -> Result<(), TryProbeError> {
        self.try_schedule_probe(peer_id)?;
        let dial_opts = DialOpts::peer_id(peer_id)
            .addresses(vec![addr])
            .condition(PeerCondition::Always)
            .build();
        let conn_id = dial_opts.connection_id();
        self.pending_outbound_conns.insert(peer_id, conn_id);
        self.pending_events
            .push_back(ToSwarm::Dial { opts: dial_opts });
        Ok(())
    }

    fn on_dial_failure(
        &mut self,
        peer_id: PeerId,
        conn_id: ConnectionId,
        error: String,
    ) -> Option<TToSwarm<Self>> {
        self.pending_outbound_conns.remove_by_right(&conn_id)?;
        log::debug!("Probe for peer {peer_id} failed: {error}");

        _ = self.probe_timeouts.remove(peer_id);
        Some(ToSwarm::GenerateEvent(BaseBehaviourEvent::PeerProbed(
            PeerProbed {
                peer_id,
                result: ProbeResult::Error(error.into_boxed_str()),
            },
        )))
    }

    fn on_probe_timeout(&mut self, peer_id: PeerId) -> TToSwarm<Self> {
        log::debug!("Probe for peer {peer_id} timed out");

        self.pending_outbound_conns.remove_by_left(&peer_id);
        ToSwarm::GenerateEvent(BaseBehaviourEvent::PeerProbed(PeerProbed {
            peer_id,
            result: ProbeResult::Timeout,
        }))
    }

    pub fn mesh_peers(&self, topic: &str) -> usize {
        self.inner.pubsub.mesh_peers(topic)
    }

    #[cfg(feature = "whitelist")]
    pub fn allow_peer(&mut self, peer_id: PeerId) {
        self.inner.whitelist.allow_peer(peer_id);
    }
}

#[derive(Debug, Clone)]
pub enum BaseBehaviourEvent {
    PeerProbed(PeerProbed),
    Gossipsub(GossipSubMessage),
}

#[derive(Debug, Clone)]
pub struct GossipSubMessage {
    pub peer_id: PeerId,
    pub topic: &'static str,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PeerProbed {
    pub peer_id: PeerId,
    pub result: ProbeResult,
}

#[derive(Debug, Clone)]
pub enum ProbeResult {
    Timeout,
    Error(Box<str>),
    Reachable {
        listen_addrs: Vec<Multiaddr>,
        agent_version: Box<str>,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum TryProbeError {
    #[error("There are too many active probes")]
    TooManyProbes,
    #[error("There is already an ongoing probe for this peer")]
    Ongoing,
}

impl BehaviourWrapper for BaseBehaviour {
    type Inner = InnerBehaviour;
    type Event = BaseBehaviourEvent;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn on_swarm_event(&mut self, ev: FromSwarm) -> impl IntoIterator<Item = TToSwarm<Self>> {
        match ev {
            FromSwarm::ConnectionEstablished(conn) => self.on_connection_established(conn),
            FromSwarm::ConnectionClosed(conn) => self.on_connection_closed(conn),
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error,
                connection_id,
            }) => self.on_dial_failure(peer_id, connection_id, error.to_string()),
            _ => None,
        }
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        match ev {
            InnerBehaviourEvent::Identify(ev) => self.on_identify_event(ev),
            InnerBehaviourEvent::Kademlia(ev) => self.on_kademlia_event(ev),
            InnerBehaviourEvent::Autonat(ev) => self.on_autonat_event(ev),
            InnerBehaviourEvent::Pubsub(ev) => self.on_pubsub_event(ev),
            InnerBehaviourEvent::Ping(_ev) => None,
            InnerBehaviourEvent::Dcutr(_ev) => None,

            #[cfg(feature = "whitelist")]
            InnerBehaviourEvent::Whitelist(nodes) => self.on_nodes_update(nodes),

            _ => None,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<impl IntoIterator<Item = TToSwarm<Self>>> {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(Some(ev));
        }

        match self.probe_timeouts.poll_unpin(cx) {
            Poll::Ready((peer_id, Err(_))) => {
                return Poll::Ready(Some(self.on_probe_timeout(peer_id)));
            }
            Poll::Pending => {}
            _ => unreachable!(), // future::pending() should never complete
        }

        Poll::Pending
    }
}

impl BaseBehaviour {
    fn on_connection_established(&mut self, conn: ConnectionEstablished) -> Option<TToSwarm<Self>> {
        let peer_id = match conn.endpoint {
            ConnectedPoint::Dialer { .. } => conn.peer_id,
            _ => return None,
        };
        log::debug!("Established outbound connection to {peer_id}");
        *self.outbound_conns.entry(peer_id).or_default() += 1;
        None
    }

    fn on_connection_closed(&mut self, conn: ConnectionClosed) -> Option<TToSwarm<Self>> {
        let peer_id = match conn.endpoint {
            ConnectedPoint::Dialer { .. } => conn.peer_id,
            _ => return None,
        };
        log::debug!("Closed outbound connection to {peer_id}");
        match self.outbound_conns.get_mut(&peer_id) {
            Some(x) => *x -= 1,
            None => log::error!("Closed connection not established before"),
        }
        None
    }

    fn on_identify_event(&mut self, ev: identify::Event) -> Option<TToSwarm<Self>> {
        log::debug!("Identify event received: {ev:?}");

        let (peer_id, listen_addrs, agent_version, conn_id) = match ev {
            identify::Event::Received {
                peer_id,
                info,
                connection_id,
            } => (
                peer_id,
                info.listen_addrs,
                info.agent_version,
                connection_id,
            ),
            _ => return None,
        };

        // Filter out unreachable (private) addresses and add the remaining to cache and DHT
        let listen_addrs = listen_addrs.into_iter().filter(addr_is_reachable);
        self.inner.address_cache.put(peer_id, listen_addrs.clone());
        listen_addrs.clone().for_each(|addr| {
            self.inner.kademlia.add_address(&peer_id, addr);
        });

        let pending_conn = self.pending_outbound_conns.get_by_left(&peer_id);
        // In case of a DHT probe, there should be no connection ID in `pending_outbound_conns`
        // In case of a direct probe, the connection ID should match the one stored
        if self.probe_timeouts.contains(peer_id)
            && (pending_conn.is_none() || pending_conn.is_some_and(|id| id == &conn_id))
        {
            self.probe_timeouts.remove(peer_id);
            self.pending_outbound_conns.remove_by_left(&peer_id);

            log::debug!("Probe for {peer_id} succeeded");
            Some(ToSwarm::GenerateEvent(BaseBehaviourEvent::PeerProbed(
                PeerProbed {
                    peer_id,
                    result: ProbeResult::Reachable {
                        listen_addrs: listen_addrs.collect(),
                        agent_version: agent_version.into_boxed_str(),
                    },
                },
            )))
        } else {
            None
        }
    }

    fn on_kademlia_event(&mut self, ev: kad::Event) -> Option<TToSwarm<Self>> {
        log::debug!("Kademlia event received: {ev:?}");

        let kad::Event::OutboundQueryProgressed {
            id: query_id,
            result: QueryResult::GetClosestPeers(result),
            step: ProgressStep { last, .. },
            ..
        } = ev
        else {
            return None;
        };

        let peer_id = self.ongoing_queries.get_by_right(&query_id)?.to_owned();
        let peer_info = match result {
            Ok(GetClosestPeersOk { peers, .. })
            | Err(GetClosestPeersError::Timeout { peers, .. }) => {
                peers.into_iter().find(|p| p.peer_id == peer_id)
            }
        };
        let query_finished = last || peer_info.is_some();

        // Query finished
        if query_finished {
            log::debug!("Query for peer {peer_id} finished.");
            self.ongoing_queries.remove_by_right(&query_id);
        }

        if let Some(peer_info) = peer_info {
            // Cache the found address(es) so they can be used for dialing
            // (kademlia might not do it by itself, if the bucket is full)
            self.inner.address_cache.put(peer_id, peer_info.addrs);
        }

        // Try to dial even if `peer_info` is `None`.
        // There might be some address(es) cached from previous queries.
        query_finished.then_some(ToSwarm::Dial {
            // Not using the default condition (`DisconnectedAndNotDialing`), because we may want
            // to establish an outbound connection to the peer despite existing inbound connection.
            opts: DialOpts::peer_id(peer_id)
                .condition(PeerCondition::NotDialing)
                .build(),
        })
    }

    fn on_autonat_event(&mut self, ev: autonat::Event) -> Option<TToSwarm<Self>> {
        log::debug!("AutoNAT event received: {ev:?}");
        let autonat::Event::StatusChanged { new: status, .. } = ev else {
            return None;
        };
        match status {
            NatStatus::Public(addr) => log::info!("Public address confirmed: {addr}"),
            NatStatus::Private => log::warn!("Public address check failed."),
            NatStatus::Unknown => {}
        }
        None
    }

    fn on_pubsub_event(
        &mut self,
        PubsubMsg {
            peer_id,
            topic,
            data,
        }: PubsubMsg,
    ) -> Option<TToSwarm<Self>> {
        log::trace!("Pub-sub message received: peer_id={peer_id} topic={topic}");
        let message = data.to_vec();

        let ev = BaseBehaviourEvent::Gossipsub(GossipSubMessage {
            peer_id,
            topic,
            message,
        });
        Some(ToSwarm::GenerateEvent(ev))
    }

    #[cfg(feature = "whitelist")]
    fn on_nodes_update(&mut self, nodes: WhitelistedPeers) -> Option<TToSwarm<Self>> {
        log::debug!("Updating registered workers");
        *self.registered_nodes.write() = nodes;
        None
    }
}
