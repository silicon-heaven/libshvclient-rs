#[cfg(not(any(feature = "tokio", feature = "async_std", feature = "smol")))]
compile_error!("No async runtime selected. Exactly one of `tokio`, `smol` or `async_std` features must be enabled.");

#[cfg(any(
        all(feature = "tokio", feature = "async_std"),
        all(feature = "tokio", feature = "smol"),
        all(feature = "smol", feature = "async_std")
))]
compile_error!("Only one of `tokio`, `smol`, `async_std` features can be enabled at a time.");

pub mod appnodes;
pub mod client;
pub mod runtime;
pub mod clientnode;
mod connection;
mod macros;

pub use client::{
    AppState,
    Client,
    ClientCommandSender,
    ClientEvent,
    ClientEventsReceiver,
};
pub use clientnode::RequestHandler;
pub use connection::ConnectionFailedKind;

// Reexport for version compatibility
pub use shvproto;
pub use shvrpc;
