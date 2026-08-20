//! Zakura request/response transport for the spendability PIR server.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use crate::nullifier::state::AppState as SpendState;
use crate::witness::state::AppState as WitnessState;
pub use pir_protocol::p2p::{
    CombinedHealthResponse, Message, P2PError, SubsystemHealth, FRAME_FLAG_MORE, PIR_CAPABILITY,
    PIR_SERVICE_ID, PIR_STREAM_KIND, PIR_STREAM_VERSION,
};
use pir_protocol::{p2p::MessageDecoder, PirEngine};
use zakura_network::zakura::{
    Frame, Peer, Service, SinkReject, Stream, StreamMode, ZakuraConnId, ZakuraPeerId,
    LOCAL_MAX_CONTROL_FRAME_BYTES,
};

pub type SpendPirEngine = crate::pir::YpirPirEngine;

pub type WitnessPirEngine = crate::pir::YpirPirEngine;

const FRAME_HEADER_BYTES: usize = 8;
const FRAME_PAYLOAD_BYTES: usize = LOCAL_MAX_CONTROL_FRAME_BYTES as usize - FRAME_HEADER_BYTES;
const PIR_STREAMS: [Stream; 1] = [Stream {
    kind: PIR_STREAM_KIND,
    version: PIR_STREAM_VERSION,
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
        Self::to_json_frame(
            &CombinedHealthResponse {
                nullifier: SubsystemHealth::from(self.spend_state.phase.load().as_ref()),
                witness: SubsystemHealth::from(self.witness_state.phase.load().as_ref()),
            },
            Message::HealthRes,
        )
    }

    async fn nullifier_metadata(&self) -> Result<Vec<Frame>, P2PError> {
        let pir = self.spend_state.live_pir.load();
        let metadata = match pir.as_ref() {
            Some(pir_state) => &pir_state.metadata,
            None => return Err(P2PError::ServiceUnavailable),
        };

        Self::to_json_frame(metadata, Message::NullifierMetadataRes)
    }

    async fn nullifier_params(&self) -> Result<Vec<Frame>, P2PError> {
        Self::to_json_frame(&self.spend_state.scenario, Message::NullifierParamsRes)
    }

    async fn nullifier_query(&self, payload: Vec<u8>) -> Result<Vec<Frame>, P2PError> {
        let pir = self.spend_state.live_pir.load();
        let pir_state = match pir.as_ref() {
            Some(pir_state) => pir_state,
            None => return Err(P2PError::ServiceUnavailable),
        };
        let payload = self
            .spend_state
            .engine
            .answer_query(&pir_state.engine_state, &payload)
            .map_err(|error| P2PError::QueryError(error.to_string()))?;

        Ok(Self::frame(payload, Message::NullifierQueryRes))
    }

    async fn witness_metadata(&self) -> Result<Vec<Frame>, P2PError> {
        let pir = self.witness_state.live_pir.load();
        let meta = match pir.as_ref() {
            Some(pir_state) => Ok(&pir_state.metadata),
            None => Err(P2PError::ServiceUnavailable),
        }?;

        P2pPirService::to_json_frame(meta, Message::WitnessMetadataRes)
    }

    pub fn to_json_frame<T>(value: &T, msg_type: Message) -> Result<Vec<Frame>, P2PError>
    where
        T: ?Sized + serde::Serialize,
    {
        let payload = serde_json::to_vec(value).map_err(|e| P2PError::SerdeError(e.to_string()))?;
        Ok(Self::frame(payload, msg_type))
    }

    fn frame(payload: Vec<u8>, msg_type: Message) -> Vec<Frame> {
        let mut frames: Vec<_> = payload
            .chunks(FRAME_PAYLOAD_BYTES)
            .map(|chunk| Frame {
                message_type: msg_type.into(),
                flags: FRAME_FLAG_MORE,
                payload: chunk.to_vec(),
            })
            .collect();
        if frames.is_empty() {
            frames.push(Frame {
                message_type: msg_type.into(),
                flags: 0,
                payload,
            });
        } else {
            frames.last_mut().expect("frames is not empty").flags = 0;
        }
        frames
    }

    async fn witness_broadcast(&self) -> Result<Vec<Frame>, P2PError> {
        let pir = self.witness_state.live_pir.load();
        let broadcast = match pir.as_ref() {
            Some(pir_state) => &pir_state.broadcast,
            None => return Err(P2PError::ServiceUnavailable),
        };

        Self::to_json_frame(broadcast, Message::WitnessBroadcastRes)
    }

    async fn witness_params(&self) -> Result<Vec<Frame>, P2PError> {
        Self::to_json_frame(&self.witness_state.scenario, Message::WitnessParamsRes)
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

        Ok(Self::frame(payload, Message::WitnessQueryRes))
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
        if !payload.is_empty()
            && matches!(
                message,
                Message::HealthReq
                    | Message::NullifierMetadataReq
                    | Message::NullifierParamsReq
                    | Message::WitnessMetadataReq
                    | Message::WitnessBroadcastReq
                    | Message::WitnessParamsReq
            )
        {
            return Err(SinkReject::protocol(format!(
                "request type {message_type} requires an empty payload"
            )));
        }

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
            Err(error) => Self::frame(
                serde_json::to_vec(&error).map_err(SinkReject::local)?,
                Message::ErrRes,
            ),
        };

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
            let mut decoder = MessageDecoder::default();
            'peer: loop {
                let frame = tokio::select! {
                    _ = service_cancel.cancelled() => break,
                    frame = recv.recv() => match frame {
                        Some(frame) => frame,
                        None => break,
                    },
                };

                let frame = match decoder.push(frame.message_type, frame.flags, frame.payload) {
                    Ok(Some((message_type, payload))) => Frame {
                        message_type: message_type.into(),
                        flags: 0,
                        payload,
                    },
                    Ok(None) => continue,
                    Err(_) => {
                        connection_cancel.cancel();
                        break;
                    }
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
    use zakura_network::{
        zakura::{spawn_zakura_endpoint_with_services, CustomService, ZakuraServiceId},
        Config, P2pStack,
    };

    #[derive(Debug)]
    struct NoopService;

    impl Service for NoopService {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn streams(&self) -> &[Stream] {
            &[]
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    #[test]
    fn oversized_payload_is_framed_and_reassembled() {
        let payload = vec![42; FRAME_PAYLOAD_BYTES + 13];
        let frames = P2pPirService::frame(payload.clone(), Message::NullifierQueryReq);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].flags, FRAME_FLAG_MORE);
        assert_eq!(frames[1].flags, 0);
        assert!(frames
            .iter()
            .all(|frame| frame.encode(LOCAL_MAX_CONTROL_FRAME_BYTES).is_ok()));

        let mut decoder = MessageDecoder::default();
        assert!(decoder
            .push(
                frames[0].message_type,
                frames[0].flags,
                frames[0].payload.clone()
            )
            .unwrap()
            .is_none());
        let decoded = decoder
            .push(
                frames[1].message_type,
                frames[1].flags,
                frames[1].payload.clone(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(decoded, (Message::NullifierQueryReq, payload));
    }

    #[tokio::test]
    async fn health_and_params_are_available_before_pir_setup() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let witness_scenario = pir_protocol::YpirScenario {
            num_items: 4_096,
            item_size_bits: 65_536,
            poly_len: 4_096,
        };
        let spend_scenario = pir_protocol::YpirScenario {
            num_items: 16_384,
            item_size_bits: 28_672,
            poly_len: 2_048,
        };
        let witness_state = Arc::new(WitnessState::new(
            crate::witness::state::ServerConfig {
                zcash_network: pir_protocol::ZcashNetwork::Main,
                snapshot_interval: 1,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: addr,
                window_shard_limit: 16,
            },
            Arc::new(WitnessPirEngine::new(&witness_scenario)),
        ));
        let spend_state = Arc::new(SpendState::new(
            crate::nullifier::state::ServerConfig {
                zcash_network: pir_protocol::ZcashNetwork::Main,
                target_size: 1,
                confirmation_depth: 1,
                snapshot_interval: 1,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: addr,
            },
            Arc::new(SpendPirEngine::new(&spend_scenario)),
        ));
        witness_state
            .phase
            .store(Arc::new(pir_protocol::ServerPhase::Serving));
        let service = P2pPirService::new(witness_state, spend_state);

        let health = service
            .forward(Frame {
                message_type: Message::HealthReq.into(),
                flags: 0,
                payload: vec![],
            })
            .await
            .unwrap();
        let health: CombinedHealthResponse = serde_json::from_slice(&health[0].payload).unwrap();
        assert_eq!(health.nullifier.phase, "syncing");
        assert_eq!(health.nullifier.current_height, Some(0));
        assert_eq!(health.witness.phase, "serving");
        assert!(matches!(
            service
                .forward(Frame {
                    message_type: Message::HealthReq.into(),
                    flags: 0,
                    payload: vec![1],
                })
                .await,
            Err(SinkReject::Protocol(_))
        ));

        let witness_params = service
            .forward(Frame {
                message_type: Message::WitnessParamsReq.into(),
                flags: 0,
                payload: vec![],
            })
            .await
            .unwrap();

        assert_eq!(
            witness_params[0].message_type,
            u16::from(Message::WitnessParamsRes)
        );
        let scenario: pir_protocol::YpirScenario =
            serde_json::from_slice(&witness_params[0].payload).unwrap();
        assert_eq!(scenario.num_items, witness_scenario.num_items);
        assert_eq!(scenario.item_size_bits, witness_scenario.item_size_bits);

        let nullifier_params = service
            .forward(Frame {
                message_type: Message::NullifierParamsReq.into(),
                flags: 0,
                payload: vec![],
            })
            .await
            .unwrap();
        assert_eq!(
            nullifier_params[0].message_type,
            u16::from(Message::NullifierParamsRes)
        );
        let scenario: pir_protocol::YpirScenario =
            serde_json::from_slice(&nullifier_params[0].payload).unwrap();
        assert_eq!(scenario.num_items, spend_scenario.num_items);
        assert_eq!(scenario.item_size_bits, spend_scenario.item_size_bits);
    }

    #[tokio::test]
    async fn loopback_client_uses_custom_service_stream() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let witness_scenario = pir_protocol::YpirScenario {
            num_items: 4_096,
            item_size_bits: 65_536,
            poly_len: 4_096,
        };
        let spend_scenario = pir_protocol::YpirScenario {
            num_items: 16_384,
            item_size_bits: 28_672,
            poly_len: 2_048,
        };
        let witness_state = Arc::new(WitnessState::new(
            crate::witness::state::ServerConfig {
                zcash_network: pir_protocol::ZcashNetwork::Main,
                snapshot_interval: 1,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: addr,
                window_shard_limit: 16,
            },
            Arc::new(WitnessPirEngine::new(&witness_scenario)),
        ));
        let spend_state = Arc::new(SpendState::new(
            crate::nullifier::state::ServerConfig {
                zcash_network: pir_protocol::ZcashNetwork::Main,
                target_size: 1,
                confirmation_depth: 1,
                snapshot_interval: 1,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: addr,
            },
            Arc::new(SpendPirEngine::new(&spend_scenario)),
        ));
        let server_identity = tempfile::tempdir().unwrap();
        let client_identity = tempfile::tempdir().unwrap();
        let mut config = Config {
            p2p_stack: P2pStack::Zakura,
            identity_dir: server_identity.path().to_owned(),
            ..Config::default()
        };
        config.zakura.listen_addr = Some(addr);
        config.zakura.bootstrap_peers.clear();
        let endpoint = spawn_zakura_endpoint_with_services(
            &config,
            |_supervisor, _trace| Arc::new(NoopService),
            None,
            vec![CustomService {
                service: Arc::new(P2pPirService::new(witness_state, spend_state)),
                provides: vec![ZakuraServiceId::new(PIR_SERVICE_ID).unwrap()],
                seeks: vec![],
            }],
        )
        .await
        .unwrap()
        .unwrap();
        let node_addr = endpoint.node_addr().await;
        let direct = node_addr
            .direct_addresses()
            .find(|address| address.ip().is_loopback())
            .unwrap();
        let (client_node, client) = spendability_pir_client::P2pPirNode::spawn(
            client_identity.path().to_owned(),
            vec![format!("{}@{direct}", node_addr.node_id)],
            pir_protocol::ZcashNetwork::Main,
        )
        .await
        .unwrap();
        let session = tokio::time::timeout(std::time::Duration::from_secs(10), client.session())
            .await
            .unwrap()
            .unwrap();

        let health: CombinedHealthResponse =
            serde_json::from_slice(&session.request(Message::HealthReq, vec![]).await.unwrap())
                .unwrap();
        assert_eq!(health.nullifier.phase, "syncing");
        let params: pir_protocol::YpirScenario = serde_json::from_slice(
            &session
                .request(Message::WitnessParamsReq, vec![])
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(params.poly_len, witness_scenario.poly_len);

        client_node.shutdown().await;
        assert!(tokio::time::timeout(
            std::time::Duration::from_secs(5),
            session.request(Message::HealthReq, vec![])
        )
        .await
        .unwrap()
        .is_err());
        endpoint.shutdown().await;
    }
}
