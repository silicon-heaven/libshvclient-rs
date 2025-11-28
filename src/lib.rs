#[cfg(not(any(feature = "tokio", feature = "smol")))]
compile_error!("No async runtime selected. Exactly one of `tokio` or `smol` features must be enabled.");

#[cfg(all(feature = "tokio", feature = "smol"))]
compile_error!("Only one of `tokio` or `smol` async runtimes can be enabled at a time.");

pub mod appnodes;
pub mod client;
pub mod runtime;
pub mod clientnode;
pub mod clientapi;
mod connection;
mod macros;

pub use client::Client;

pub use clientapi::{
    ClientCommandSender,
    ClientEvent,
    ClientEventsReceiver,
};

pub use clientnode::{DynamicNodeHandler, StaticNodeHandler};
pub use connection::ConnectionFailedKind;

// Reexport for version compatibility
pub use shvproto;
pub use shvrpc;
