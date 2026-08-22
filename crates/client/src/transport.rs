use crate::p2p::{P2pClientError, P2pPirSession};
use pir_protocol::p2p::Message;

#[derive(Clone)]
pub(crate) enum PirTransport {
    Http {
        client: reqwest::Client,
        base_url: String,
    },
    Zakura(P2pPirSession),
}

#[derive(Clone, Copy)]
pub(crate) enum Operation {
    NullifierMetadata,
    NullifierParams,
    NullifierQuery,
    WitnessMetadata,
    WitnessBroadcast,
    WitnessParams,
    WitnessQuery,
}

impl Operation {
    fn path(self) -> &'static str {
        match self {
            Self::NullifierMetadata | Self::WitnessMetadata => "metadata",
            Self::NullifierParams | Self::WitnessParams => "params",
            Self::NullifierQuery | Self::WitnessQuery => "query",
            Self::WitnessBroadcast => "broadcast",
        }
    }

    fn message(self) -> Message {
        match self {
            Self::NullifierMetadata => Message::NullifierMetadataReq,
            Self::NullifierParams => Message::NullifierParamsReq,
            Self::NullifierQuery => Message::NullifierQueryReq,
            Self::WitnessMetadata => Message::WitnessMetadataReq,
            Self::WitnessBroadcast => Message::WitnessBroadcastReq,
            Self::WitnessParams => Message::WitnessParamsReq,
            Self::WitnessQuery => Message::WitnessQueryReq,
        }
    }

    fn is_query(self) -> bool {
        matches!(self, Self::NullifierQuery | Self::WitnessQuery)
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum TransportError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    P2p(#[from] P2pClientError),
    #[error("server unavailable")]
    Unavailable,
}

impl PirTransport {
    pub(crate) fn http(url: &str) -> Self {
        Self::Http {
            client: reqwest::Client::new(),
            base_url: url.trim_end_matches('/').to_string(),
        }
    }

    pub(crate) async fn request(
        &self,
        operation: Operation,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, TransportError> {
        match self {
            Self::Http { client, base_url } => {
                let request = if operation.is_query() {
                    client
                        .post(format!("{base_url}/{}", operation.path()))
                        .body(payload)
                } else {
                    client.get(format!("{base_url}/{}", operation.path()))
                };
                let response = request.send().await?;
                if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    return Err(TransportError::Unavailable);
                }
                Ok(response.error_for_status()?.bytes().await?.to_vec())
            }
            Self::Zakura(session) => Ok(session.request(operation.message(), payload).await?),
        }
    }
}
