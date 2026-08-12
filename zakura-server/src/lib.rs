#![cfg(feature = "ypir")]

//! Zakura request/response transport for the spendability PIR server.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use pir_types::PirEngine;
use spend_server::state::AppState as SpendState;
use witness_server::state::AppState as WitnessState;
use zakura_network::zakura::{
    Frame, Peer, Service, SinkReject, Stream, StreamMode, ZakuraConnId, ZakuraPeerId,
    LOCAL_MAX_CONTROL_FRAME_BYTES,
};

pub type SpendPirEngine = spend_server::pir_ypir::YpirPirEngine;

pub type WitnessPirEngine = witness_server::pir_ypir::YpirPirEngine;

// TODO: what is this for?
pub const PIR_CAPABILITY: u64 = 1 << 16;

/// Request/response stream carrying all spendability PIR operations.
pub const PIR_STREAM_KIND: u16 = 64;
pub const PIR_STREAM_VERSION: u16 = 1;
pub const PIR_SERVICE_ID: &str = "zakura.spendability-pir.v1";

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum P2PError {
    #[error("invalid message type: {0}")]
    InvalidMessageType(u16),
    #[error("service currently unavailable")]
    ServiceUnavailable,
    #[error("serde error: {0}")]
    SerdeError(String),
    #[error("PIR query failed: {0}")]
    QueryError(String),
}

impl From<serde_json::Error> for P2PError {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeError(value.to_string())
    }
}

impl From<P2PError> for SinkReject {
    fn from(value: P2PError) -> Self {
        SinkReject::protocol(value)
    }
}

/// Spendability PIR wire message types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Message {
    HealthReq,
    NullifierMetadataReq,
    NullifierParamsReq,
    NullifierQueryReq,
    WitnessMetadataReq,
    WitnessBroadcastReq,
    WitnessParamsReq,
    WitnessQueryReq,

    HealthRes,
    NullifierMetadataRes,
    NullifierParamsRes,
    NullifierQueryRes,
    WitnessMetadataRes,
    WitnessBroadcastRes,
    WitnessParamsRes,
    WitnessQueryRes,

    ErrRes,
}

impl From<Message> for u16 {
    fn from(message: Message) -> Self {
        message as u16
    }
}

impl TryFrom<u16> for Message {
    type Error = P2PError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::HealthReq),
            1 => Ok(Self::NullifierMetadataReq),
            2 => Ok(Self::NullifierParamsReq),
            3 => Ok(Self::NullifierQueryReq),
            4 => Ok(Self::WitnessMetadataReq),
            5 => Ok(Self::WitnessBroadcastReq),
            6 => Ok(Self::WitnessParamsReq),
            7 => Ok(Self::WitnessQueryReq),
            8 => Ok(Self::HealthRes),
            9 => Ok(Self::NullifierMetadataRes),
            10 => Ok(Self::NullifierParamsRes),
            11 => Ok(Self::NullifierQueryRes),
            12 => Ok(Self::WitnessMetadataRes),
            13 => Ok(Self::WitnessBroadcastRes),
            14 => Ok(Self::WitnessParamsRes),
            15 => Ok(Self::WitnessQueryRes),
            16 => Ok(Self::ErrRes),
            other => Err(P2PError::InvalidMessageType(other)),
        }
    }
}

const FRAME_HEADER_BYTES: usize = 8;
const PIR_STREAMS: [Stream; 1] = [Stream {
    kind: PIR_STREAM_KIND,
    version: PIR_STREAM_VERSION,
    // YPIR query uploads are approximately 600-700 KiB. Zakura currently
    // applies its 1 MiB custom/control cap to application-defined streams.
    frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
    capability: PIR_CAPABILITY,
    mode: StreamMode::Ordered,
}];

/// Native Zakura frontend for spendability PIR.
#[derive(Clone)]
pub struct P2pPirService {
    active_peers: Arc<Mutex<HashSet<(ZakuraPeerId, ZakuraConnId)>>>,
    witness_state: Arc<WitnessState<WitnessPirEngine>>,
    spend_state: Arc<SpendState<SpendPirEngine>>,
}

impl std::fmt::Debug for P2pPirService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("P2pPirService")
            .field("active_peers", &self.active_peers)
            .finish()
    }
}

impl P2pPirService {
    pub fn new(
        witness_state: Arc<WitnessState<WitnessPirEngine>>,
        spend_state: Arc<SpendState<SpendPirEngine>>,
    ) -> Self {
        Self {
            active_peers: Arc::new(Mutex::new(HashSet::new())),
            witness_state,
            spend_state,
        }
    }

    async fn health(&self) -> Result<Vec<Frame>, P2PError> {
        Err(P2PError::ServiceUnavailable)
    }

    async fn nullifier_metadata(&self) -> Result<Vec<Frame>, P2PError> {
        Err(P2PError::ServiceUnavailable)
    }

    async fn nullifier_params(&self) -> Result<Vec<Frame>, P2PError> {
        Err(P2PError::ServiceUnavailable)
    }

    async fn nullifier_query(&self, _payload: Vec<u8>) -> Result<Vec<Frame>, P2PError> {
        Err(P2PError::ServiceUnavailable)
    }

    async fn witness_metadata(&self) -> Result<Vec<Frame>, P2PError> {
        let pir = self.witness_state.live_pir.load();
        let meta = match pir.as_ref() {
            Some(pir_state) => Ok(&pir_state.metadata),
            None => Err(P2PError::ServiceUnavailable),
        }?;

        P2pPirService::to_frame(meta, Message::WitnessMetadataRes)
    }

    pub fn to_frame<T>(value: &T, msg_type: Message) -> Result<Vec<Frame>, P2PError>
    where
        T: ?Sized + serde::Serialize,
    {
        let payload = serde_json::to_vec(value).map_err(|e| P2PError::SerdeError(e.to_string()))?;
        Ok(vec![Frame {
            message_type: msg_type as u16,
            flags: 0,
            payload,
        }])
    }

    async fn witness_broadcast(&self) -> Result<Vec<Frame>, P2PError> {
        let pir = self.witness_state.live_pir.load();
        let broadcast = match pir.as_ref() {
            Some(pir_state) => &pir_state.broadcast,
            None => return Err(P2PError::ServiceUnavailable),
        };

        Self::to_frame(broadcast, Message::WitnessBroadcastRes)
    }

    async fn witness_params(&self) -> Result<Vec<Frame>, P2PError> {
        Self::to_frame(&self.witness_state.scenario, Message::WitnessParamsRes)
    }

    async fn witness_query(&self, payload: Vec<u8>) -> Result<Vec<Frame>, P2PError> {
        let pir = self.witness_state.live_pir.load();
        let pir_state = match pir.as_ref() {
            Some(pir_state) => pir_state,
            None => return Err(P2PError::ServiceUnavailable),
        };
        let payload = self
            .witness_state
            .engine
            .answer_query(&pir_state.engine_state, &payload)
            .map_err(|error| P2PError::QueryError(error.to_string()))?;

        Ok(vec![Frame {
            message_type: Message::WitnessQueryRes.into(),
            flags: 0,
            payload,
        }])
    }

    async fn forward(&self, frame: Frame) -> Result<Vec<Frame>, SinkReject> {
        if frame.flags != 0 {
            return Err(SinkReject::protocol(format!(
                "request flags must be zero, received {}",
                frame.flags
            )));
        }

        let message_type = frame.message_type;
        let payload = frame.payload;
        let message = Message::try_from(message_type).map_err(SinkReject::protocol)?;

        let result = match message {
            Message::HealthReq => self.health().await,
            Message::NullifierMetadataReq => self.nullifier_metadata().await,
            Message::NullifierParamsReq => self.nullifier_params().await,
            Message::NullifierQueryReq => self.nullifier_query(payload).await,
            Message::WitnessMetadataReq => self.witness_metadata().await,
            Message::WitnessBroadcastReq => self.witness_broadcast().await,
            Message::WitnessParamsReq => self.witness_params().await,
            Message::WitnessQueryReq => self.witness_query(payload).await,
            _ => {
                return Err(SinkReject::protocol(format!(
                    "expected a request message, received {message_type}"
                )))
            }
        };

        let frames = match result {
            Ok(frames) => frames,
            Err(error) => vec![Frame {
                message_type: Message::ErrRes.into(),
                flags: 0,
                payload: serde_json::to_vec(&error).map_err(SinkReject::local)?,
            }],
        };

        let payload_cap = usize::try_from(PIR_STREAMS[0].frame_cap)
            .unwrap_or(usize::MAX)
            .saturating_sub(FRAME_HEADER_BYTES);
        if let Some(frame) = frames
            .iter()
            .find(|frame| frame.payload.len() > payload_cap)
        {
            return Err(SinkReject::local(format!(
                "response payload is {} bytes, exceeding negotiated cap of {payload_cap} bytes",
                frame.payload.len()
            )));
        }

        Ok(frames)
    }
}

impl Service for P2pPirService {
    fn name(&self) -> &'static str {
        "spendability-pir"
    }

    fn streams(&self) -> &[Stream] {
        &PIR_STREAMS
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.active_peers
            .lock()
            .is_ok_and(|peers| peers.contains(&(peer.clone(), conn_id)))
    }

    fn add_peer(&self, mut peer: Peer) {
        let peer_id = peer.id.clone();
        let conn_id = peer.conn_id;
        let connection_cancel = peer.cancel_token();
        let service_cancel = peer.service_cancel_token();
        let Some((mut recv, send)) = peer.take_stream(PIR_STREAM_KIND) else {
            return;
        };

        if let Ok(mut peers) = self.active_peers.lock() {
            peers.insert((peer_id.clone(), conn_id));
        }

        let service = self.clone();
        tokio::spawn(async move {
            'peer: loop {
                let frame = tokio::select! {
                    _ = service_cancel.cancelled() => break,
                    frame = recv.recv() => match frame {
                        Some(frame) => frame,
                        None => break,
                    },
                };

                let responses = match service.forward(frame).await {
                    Ok(responses) => responses,
                    Err(SinkReject::Protocol(_)) => {
                        connection_cancel.cancel();
                        break;
                    }
                    Err(SinkReject::Local(_)) => break,
                };

                for response in responses {
                    if send.send(response).await.is_err() {
                        break 'peer;
                    }
                }
            }

            if let Ok(mut peers) = service.active_peers.lock() {
                peers.remove(&(peer_id, conn_id));
            }
        });
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        if let Ok(mut peers) = self.active_peers.lock() {
            peers.remove(&(peer.clone(), conn_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::SocketAddr, path::PathBuf};

    #[tokio::test]
    async fn witness_params_are_available_before_pir_setup() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let witness_scenario = pir_types::YpirScenario {
            num_items: 8_192,
            item_size_bits: 65_536,
        };
        let spend_scenario = pir_types::YpirScenario {
            num_items: 16_384,
            item_size_bits: 36_736,
        };
        let witness_state = Arc::new(WitnessState::new(
            witness_server::state::ServerConfig {
                snapshot_interval: 1,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: addr,
                window_shard_limit: 32,
            },
            Arc::new(WitnessPirEngine::new(&witness_scenario)),
        ));
        let spend_state = Arc::new(SpendState::new(
            spend_server::state::ServerConfig {
                target_size: 1,
                confirmation_depth: 1,
                snapshot_interval: 1,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: addr,
            },
            Arc::new(SpendPirEngine::new(&spend_scenario)),
        ));
        let service = P2pPirService::new(witness_state, spend_state);

        let frames = service
            .forward(Frame {
                message_type: Message::WitnessParamsReq.into(),
                flags: 0,
                payload: vec![],
            })
            .await
            .unwrap();

        assert_eq!(frames[0].message_type, u16::from(Message::WitnessParamsRes));
        let scenario: pir_types::YpirScenario = serde_json::from_slice(&frames[0].payload).unwrap();
        assert_eq!(scenario.num_items, witness_scenario.num_items);
        assert_eq!(scenario.item_size_bits, witness_scenario.item_size_bits);
    }
}
