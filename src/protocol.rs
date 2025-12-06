#[cfg(feature = "whitelist")]
use crate::behaviour::whitelist::WhitelistProvider;
use libp2p::StreamProtocol;

pub trait Protocol {
    #[cfg(feature = "whitelist")]
    type NodesProvider: WhitelistProvider;

    /// Returns the unique identifier for the protocol.
    fn id(&self) -> String;
    /// Returns the DHT protocol stream protocol.
    fn dht_protocol(&self) -> StreamProtocol;

    /// Returns a whitelist provider that fetches the whitelisted nodes at the specified interval.
    #[cfg(feature = "whitelist")]
    fn whitelistst_provider(&self) -> Self::NodesProvider;
}
