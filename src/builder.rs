use std::time::Duration;

use crate::{
    behaviour::{
        base::{BaseBehaviour, BaseConfig},
        wrapped::Wrapped,
    },
    cli::{BootNode, TransportArgs},
    protocol::Protocol,
    utils::{get_keypair, parse_env_var},
    AgentInfo, Error,
};
#[allow(unused_imports)]
use futures_core::Stream;
use libp2p::{
    identity::Keypair,
    multiaddr::Protocol as Libp2pProtocol,
    noise,
    swarm::{dial_opts::DialOpts, NetworkBehaviour},
    yamux, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicConfig {
    /// Maximum transmission unit to use during MTU discovery (default: 1452).
    pub mtu_discovery_max: u16,
    /// Interval for sending keep-alive packets in milliseconds (default: 5000).
    pub keep_alive_interval_ms: u32,
    /// Timeout after which idle connections are closed in milliseconds (default: 60000).
    pub max_idle_timeout_ms: u32,
}

impl QuicConfig {
    pub fn from_env() -> Self {
        let mtu_discovery_max = parse_env_var("MTU_DISCOVERY_MAX", 1452);
        let keep_alive_interval_ms = parse_env_var("KEEP_ALIVE_INTERVAL_MS", 5000);
        let max_idle_timeout_ms = parse_env_var("MAX_IDLE_TIMEOUT_MS", 60000);
        Self {
            mtu_discovery_max,
            keep_alive_interval_ms,
            max_idle_timeout_ms,
        }
    }
}

pub struct P2PTransportBuilder<P> {
    keypair: Keypair,
    listen_addrs: Vec<Multiaddr>,
    public_addrs: Vec<Multiaddr>,
    boot_nodes: Vec<BootNode>,
    relay_addrs: Vec<Multiaddr>,
    relay: bool,
    quic_config: QuicConfig,
    base_config: BaseConfig,
    protocol: P,
    agent_info: AgentInfo,
}

impl<P: Protocol> P2PTransportBuilder<P> {
    pub fn from_cli(
        args: TransportArgs,
        protocol: P,
        agent_info: AgentInfo,
    ) -> anyhow::Result<Self> {
        let listen_addrs = args.listen_addrs();
        let keypair = get_keypair(Some(args.key))?;
        Ok(Self {
            keypair,
            listen_addrs,
            public_addrs: args.p2p_public_addrs,
            boot_nodes: args.boot_nodes,
            relay_addrs: vec![],
            relay: false,
            quic_config: QuicConfig::from_env(),
            base_config: BaseConfig::from_env(),
            protocol,
            agent_info,
        })
    }

    pub fn with_listen_addrs<I: IntoIterator<Item = Multiaddr>>(mut self, addrs: I) -> Self {
        self.listen_addrs.extend(addrs);
        self
    }

    pub fn with_public_addrs<I: IntoIterator<Item = Multiaddr>>(mut self, addrs: I) -> Self {
        self.public_addrs.extend(addrs);
        self
    }

    pub fn with_boot_nodes<I: IntoIterator<Item = BootNode>>(mut self, nodes: I) -> Self {
        self.boot_nodes.extend(nodes);
        self
    }

    pub fn with_relay(mut self, relay: bool) -> Self {
        self.relay = relay;
        self
    }

    pub fn with_relay_addrs<I: IntoIterator<Item = Multiaddr>>(mut self, addrs: I) -> Self {
        self.relay_addrs.extend(addrs);
        self.relay = true;
        self
    }

    pub fn with_quic_config(mut self, f: impl FnOnce(QuicConfig) -> QuicConfig) -> Self {
        self.quic_config = f(self.quic_config);
        self
    }

    pub fn with_base_config(mut self, f: impl FnOnce(BaseConfig) -> BaseConfig) -> Self {
        self.base_config = f(self.base_config);
        self
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.keypair.public().to_peer_id()
    }

    pub fn keypair(&self) -> Keypair {
        self.keypair.clone()
    }

    fn build_swarm<T: NetworkBehaviour>(
        mut self,
        behaviour: impl FnOnce(BaseBehaviour) -> T,
    ) -> Result<Swarm<T>, Error> {
        let mut swarm = SwarmBuilder::with_existing_identity(self.keypair)
            .with_tokio()
            .with_quic_config(|config| {
                let mut config = config.mtu_upper_bound(self.quic_config.mtu_discovery_max);
                config.keep_alive_interval =
                    Duration::from_millis(self.quic_config.keep_alive_interval_ms as u64);
                config.max_idle_timeout = self.quic_config.max_idle_timeout_ms;
                config
            })
            .with_dns()?
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|keypair: &Keypair, relay| {
                let base = BaseBehaviour::new(
                    keypair,
                    self.base_config,
                    self.boot_nodes.clone(),
                    relay,
                    self.protocol,
                    self.agent_info,
                );
                behaviour(base)
            })
            .expect("infallible")
            .build();

        // If relay node not specified explicitly, use boot nodes
        if self.relay && self.relay_addrs.is_empty() {
            self.relay_addrs = self
                .boot_nodes
                .iter()
                .map(|bn| bn.address.clone().with(Libp2pProtocol::P2p(bn.peer_id)))
                .collect();
        }

        // Listen on provided addresses
        for addr in self.listen_addrs {
            swarm.listen_on(addr)?;
        }

        // Register public addresses
        for addr in self.public_addrs {
            swarm.add_external_address(addr);
        }

        // Connect to boot nodes
        for BootNode { peer_id, address } in self.boot_nodes {
            log::debug!("Connecting to boot node {peer_id} at {address}");
            swarm.dial(DialOpts::peer_id(peer_id).addresses(vec![address]).build())?;
        }

        // Connect to relay and listen for relayed connections
        if self.relay {
            for addr in self.relay_addrs {
                log::info!("Connecting to relay {addr}");
                swarm.listen_on(addr.with(Libp2pProtocol::P2pCircuit))?;
            }
        }

        Ok(swarm)
    }

    pub fn build_default_swarm(self) -> Result<Swarm<Wrapped<BaseBehaviour>>, Error> {
        let swarm = self.build_swarm(|base| base.into())?;

        Ok(swarm)
    }
}
