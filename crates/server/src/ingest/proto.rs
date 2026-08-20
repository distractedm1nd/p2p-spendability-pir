//! Re-exported protobuf types generated from `compact_formats.proto` and `service.proto`.

pub mod compact_formats {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");
}

pub use compact_formats::*;
