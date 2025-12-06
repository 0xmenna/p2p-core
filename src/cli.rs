use clap::Args;
use libp2p::{
    identity::{self, ed25519},
    multiaddr::multiaddr,
    Multiaddr, PeerId,
};
use std::{fs::File, io::Write, path::PathBuf, str::FromStr};

pub const TESTING_PEERS_FILE: &str = "testnet/authority_peers";

#[derive(Args, Clone)]
pub struct TransportArgs {
    #[arg(short, long, env = "KEY_PATH", help = "Path to libp2p key file")]
    pub key: PathBuf,

    #[arg(
        long,
        env,
        help = "Addresses on which the p2p node will listen",
        value_delimiter = ','
    )]
    pub p2p_listen_addrs: Vec<Multiaddr>,

    #[arg(
        long,
        env,
        help = "Public address(es) on which the p2p node can be reached",
        value_delimiter = ','
    )]
    pub p2p_public_addrs: Vec<Multiaddr>,

    #[arg(
        long,
        env,
        help = "Connect to boot node '<peer_id> <address>'.",
        value_delimiter = ',',
        num_args = 1..,
    )]
    pub boot_nodes: Vec<BootNode>,

    #[arg(
        long,
        env,
        default_value = "/dht/testnet/1.0.0",
        help = "Network to join"
    )]
    pub network: String,

    #[arg(long, env, default_value = "/testing/1.0.0", help = "The protocol ID")]
    pub protocol_id: String,

    #[arg(short, long, help = "Path to libp2p key file")]
    pub whitelist: PathBuf,
}

impl TransportArgs {
    pub fn listen_addrs(&self) -> Vec<Multiaddr> {
        self.p2p_listen_addrs.clone()
    }

    pub fn testing_args(nodes: u16) -> Vec<TransportArgs> {
        let base_port = 9000;
        let mut args = Vec::new();
        let mut peer_infos = Vec::new();

        let mut authority_file =
            File::create("testnet/whitelist").expect("Failed to create authority file");

        for i in 0..nodes {
            // Generate a random keypair
            let keypair = ed25519::Keypair::generate();
            let identity_keypair: identity::Keypair = keypair.clone().into();
            let peer_id = PeerId::from(identity_keypair.public());

            // Save peer_id to file
            writeln!(authority_file, "{peer_id}").unwrap();

            let port = base_port + i;
            let addr: Multiaddr = multiaddr!(Ip4([127, 0, 0, 1]), Udp(port), QuicV1);
            let key_path = PathBuf::from(format!("testnet/{peer_id}-key"));

            let mut key_file = File::create(&key_path).expect("Failed to create key file");

            key_file
                .write_all(keypair.to_bytes().as_ref())
                .expect("Failed to write key file");

            peer_infos.push((peer_id, addr.clone()));

            let boot_nodes = if i == 0 {
                vec![]
            } else {
                let (boot_peer, boot_addr) = &peer_infos[0];
                vec![BootNode {
                    peer_id: *boot_peer,
                    address: boot_addr.clone(),
                }]
            };

            args.push(TransportArgs {
                key: key_path,
                p2p_listen_addrs: vec![addr.clone()],
                p2p_public_addrs: vec![addr],
                boot_nodes,
                network: "dht/testnet/1.0.0".to_string(),
                protocol_id: "/testing/1.0.0".to_string(),
                whitelist: PathBuf::from("testnet/whitelist"),
            });
        }

        args
    }
}

#[derive(Debug, Clone)]
pub struct BootNode {
    pub peer_id: PeerId,
    pub address: Multiaddr,
}

impl FromStr for BootNode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();
        let peer_id = parts
            .next()
            .ok_or("Boot node peer ID missing")?
            .parse()
            .map_err(|_| "Invalid peer ID")?;
        let address = parts
            .next()
            .ok_or("Boot node address missing")?
            .parse()
            .map_err(|_| "Invalid address")?;
        Ok(Self { peer_id, address })
    }
}
