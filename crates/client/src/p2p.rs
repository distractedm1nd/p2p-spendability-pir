use pir_protocol::{
    p2p::{
        CombinedHealthResponse, Message, MessageDecoder, P2PError, FRAME_FLAG_MORE,
        MAX_MESSAGE_BYTES, PIR_CAPABILITY, PIR_SERVICE_ID, PIR_STREAM_KIND, PIR_STREAM_VERSION,
    },
    ZcashNetwork,
};
use std::{
    io::ErrorKind,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use zakura_network::{
    zakura::{
        spawn_zakura_endpoint_with_services, CustomService, Frame, FramedRecv, FramedSend, Peer,
        Service, Stream, StreamMode, ZakuraConnId, ZakuraEndpoint, ZakuraPeerId, ZakuraServiceId,
        LOCAL_MAX_CONTROL_FRAME_BYTES,
    },
    Config, P2pStack,
};

const FRAME_PAYLOAD_BYTES: usize = LOCAL_MAX_CONTROL_FRAME_BYTES as usize - 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const STREAMS: [Stream; 1] = [Stream {
    kind: PIR_STREAM_KIND,
    version: PIR_STREAM_VERSION,
    frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
    capability: PIR_CAPABILITY,
    mode: StreamMode::Ordered,
}];

#[derive(thiserror::Error, Debug)]
pub enum P2pClientError {
    #[error("no PIR provider is connected")]
    Disconnected,
    #[error("PIR request timed out")]
    Timeout,
    #[error("invalid PIR request message: {0:?}")]
    InvalidRequest(Message),
    #[error("PIR protocol error: {0}")]
    Protocol(String),
    #[error("PIR provider error: {0}")]
    Remote(#[source] P2PError),
    #[error("failed to start Zakura: {0}")]
    Start(String),
}

struct Request {
    message: Message,
    payload: Vec<u8>,
    response: oneshot::Sender<Result<Vec<u8>, P2pClientError>>,
}

#[derive(Clone)]
pub struct P2pPirSession {
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    requests: mpsc::Sender<Request>,
}

impl std::fmt::Debug for P2pPirSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("P2pPirSession")
            .field("peer_id", &self.peer_id)
            .field("conn_id", &self.conn_id)
            .finish()
    }
}

impl P2pPirSession {
    pub async fn health(&self) -> Result<CombinedHealthResponse, P2pClientError> {
        serde_json::from_slice(&self.request(Message::HealthReq, vec![]).await?)
            .map_err(|error| P2pClientError::Protocol(error.to_string()))
    }

    pub async fn request(
        &self,
        request: Message,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, P2pClientError> {
        if request.response().is_none() {
            return Err(P2pClientError::InvalidRequest(request));
        }
        if payload.len() > MAX_MESSAGE_BYTES {
            return Err(P2pClientError::Protocol(
                P2PError::MessageTooLarge.to_string(),
            ));
        }
        let (response, receiver) = oneshot::channel();
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            self.requests
                .send(Request {
                    message: request,
                    payload,
                    response,
                })
                .await
                .map_err(|_| P2pClientError::Disconnected)?;
            receiver.await.map_err(|_| P2pClientError::Disconnected)?
        })
        .await
        .map_err(|_| P2pClientError::Timeout)?
    }
}

#[derive(Clone)]
pub struct P2pPirClient {
    sessions: watch::Receiver<Option<P2pPirSession>>,
}

impl P2pPirClient {
    pub async fn session(&self) -> Result<P2pPirSession, P2pClientError> {
        let mut sessions = self.sessions.clone();
        loop {
            if let Some(session) = sessions.borrow().clone() {
                return Ok(session);
            }
            sessions
                .changed()
                .await
                .map_err(|_| P2pClientError::Disconnected)?;
        }
    }
}

struct Generation {
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    cancel: CancellationToken,
}

#[derive(Clone)]
struct ClientService {
    sessions: watch::Sender<Option<P2pPirSession>>,
    current: Arc<Mutex<Option<Generation>>>,
}

impl ClientService {
    fn new() -> (Arc<Self>, P2pPirClient) {
        let (sessions, receiver) = watch::channel(None);
        (
            Arc::new(Self {
                sessions,
                current: Arc::new(Mutex::new(None)),
            }),
            P2pPirClient { sessions: receiver },
        )
    }

    fn remove_generation(&self, peer_id: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if current
            .as_ref()
            .is_some_and(|current| current.peer_id == *peer_id && current.conn_id == conn_id)
        {
            current
                .take()
                .expect("generation is present")
                .cancel
                .cancel();
            self.sessions.send_replace(None);
        }
    }
}

impl std::fmt::Debug for ClientService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("P2pPirClientService")
    }
}

impl Service for ClientService {
    fn name(&self) -> &'static str {
        "spendability-pir-client"
    }

    fn streams(&self) -> &[Stream] {
        &STREAMS
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.current.lock().is_ok_and(|current| {
            current
                .as_ref()
                .is_some_and(|current| current.peer_id == *peer && current.conn_id == conn_id)
        })
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((recv, send)) = peer.take_stream(PIR_STREAM_KIND) else {
            return;
        };
        let peer_id = peer.id.clone();
        let conn_id = peer.conn_id;
        let connection_cancel = peer.cancel_token();
        let service_cancel = peer.service_cancel_token();
        let worker_cancel = CancellationToken::new();
        let (requests, receiver) = mpsc::channel(1);
        let session = P2pPirSession {
            peer_id: peer_id.clone(),
            conn_id,
            requests,
        };

        {
            let mut current = self
                .current
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if current
                .as_ref()
                .is_some_and(|current| current.conn_id >= conn_id)
            {
                service_cancel.cancel();
                return;
            }
            if let Some(old) = current.replace(Generation {
                peer_id: peer_id.clone(),
                conn_id,
                cancel: worker_cancel.clone(),
            }) {
                old.cancel.cancel();
            }
            self.sessions.send_replace(Some(session));
        }

        let service = self.clone();
        tokio::spawn(async move {
            run_worker(
                recv,
                send,
                receiver,
                connection_cancel,
                service_cancel,
                worker_cancel,
            )
            .await;
            service.remove_generation(&peer_id, conn_id);
        });
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.remove_generation(peer, conn_id);
    }
}

async fn run_worker(
    mut recv: FramedRecv,
    send: FramedSend,
    mut requests: mpsc::Receiver<Request>,
    connection_cancel: CancellationToken,
    service_cancel: CancellationToken,
    worker_cancel: CancellationToken,
) {
    loop {
        let request = tokio::select! {
            _ = connection_cancel.cancelled() => break,
            _ = service_cancel.cancelled() => break,
            _ = worker_cancel.cancelled() => break,
            request = requests.recv() => match request {
                Some(request) => request,
                None => break,
            },
        };
        let result = tokio::select! {
            _ = connection_cancel.cancelled() => Err(P2pClientError::Disconnected),
            _ = service_cancel.cancelled() => Err(P2pClientError::Disconnected),
            _ = worker_cancel.cancelled() => Err(P2pClientError::Disconnected),
            result = tokio::time::timeout(
                REQUEST_TIMEOUT,
                exchange(&mut recv, &send, request.message, request.payload),
            ) => result.unwrap_or(Err(P2pClientError::Timeout)),
        };
        let fatal = matches!(
            result,
            Err(P2pClientError::Disconnected
                | P2pClientError::Protocol(_)
                | P2pClientError::Timeout)
        );
        let _ = request.response.send(result);
        if fatal {
            connection_cancel.cancel();
            break;
        }
    }
}

async fn exchange(
    recv: &mut FramedRecv,
    send: &FramedSend,
    request: Message,
    payload: Vec<u8>,
) -> Result<Vec<u8>, P2pClientError> {
    for frame in frames(request, payload) {
        send.send(frame)
            .await
            .map_err(|_| P2pClientError::Disconnected)?;
    }

    let mut decoder = MessageDecoder::default();
    let (response, payload) = loop {
        let frame = recv.recv().await.ok_or(P2pClientError::Disconnected)?;
        if let Some(message) = decoder
            .push(frame.message_type, frame.flags, frame.payload)
            .map_err(|error| P2pClientError::Protocol(error.to_string()))?
        {
            break message;
        }
    };
    if response == Message::ErrRes {
        return Err(P2pClientError::Remote(
            serde_json::from_slice(&payload)
                .map_err(|error| P2pClientError::Protocol(error.to_string()))?,
        ));
    }
    let expected = request
        .response()
        .ok_or(P2pClientError::InvalidRequest(request))?;
    if response != expected {
        return Err(P2pClientError::Protocol(format!(
            "expected {expected:?}, received {response:?}"
        )));
    }
    Ok(payload)
}

fn frames(message: Message, payload: Vec<u8>) -> Vec<Frame> {
    let mut frames: Vec<_> = payload
        .chunks(FRAME_PAYLOAD_BYTES)
        .map(|payload| Frame {
            message_type: message.into(),
            flags: FRAME_FLAG_MORE,
            payload: payload.to_vec(),
        })
        .collect();
    if let Some(last) = frames.last_mut() {
        last.flags = 0;
    } else {
        frames.push(Frame {
            message_type: message.into(),
            flags: 0,
            payload,
        });
    }
    frames
}

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

pub struct P2pPirNode {
    endpoint: ZakuraEndpoint,
    ephemeral_identity_dir: Option<PathBuf>,
}

impl P2pPirNode {
    pub async fn spawn_ephemeral(
        bootstrap_peers: Vec<String>,
        zcash_network: ZcashNetwork,
    ) -> Result<(Self, P2pPirClient), P2pClientError> {
        let identity_dir = ephemeral_identity_dir()?;
        match Self::spawn(identity_dir.clone(), bootstrap_peers, zcash_network).await {
            Ok((mut node, client)) => {
                node.ephemeral_identity_dir = Some(identity_dir);
                Ok((node, client))
            }
            Err(error) => {
                let _ = std::fs::remove_dir_all(identity_dir);
                Err(error)
            }
        }
    }

    pub async fn spawn(
        identity_dir: PathBuf,
        bootstrap_peers: Vec<String>,
        zcash_network: ZcashNetwork,
    ) -> Result<(Self, P2pPirClient), P2pClientError> {
        let network = match zcash_network {
            ZcashNetwork::Main => "Mainnet",
            ZcashNetwork::Test => "Testnet",
        }
        .parse()
        .map_err(|error| P2pClientError::Start(format!("{error}")))?;
        let mut config = Config {
            identity_dir,
            p2p_stack: P2pStack::Zakura,
            network,
            ..Config::default()
        };
        config.zakura.listen_addr = None;
        config.zakura.bootstrap_peers = bootstrap_peers;

        let (service, client) = ClientService::new();
        let service_id = ZakuraServiceId::new(PIR_SERVICE_ID)
            .map_err(|error| P2pClientError::Start(error.to_string()))?;
        let endpoint = spawn_zakura_endpoint_with_services(
            &config,
            |_supervisor, _trace| Arc::new(NoopService),
            None,
            vec![CustomService {
                service,
                provides: vec![],
                seeks: vec![service_id],
            }],
        )
        .await
        .map_err(|error| P2pClientError::Start(error.to_string()))?
        .ok_or_else(|| P2pClientError::Start("Zakura P2P is disabled".into()))?;
        Ok((
            Self {
                endpoint,
                ephemeral_identity_dir: None,
            },
            client,
        ))
    }

    pub async fn shutdown(mut self) {
        self.endpoint.shutdown().await;
        if let Some(identity_dir) = self.ephemeral_identity_dir.take() {
            let _ = tokio::fs::remove_dir_all(identity_dir).await;
        }
    }
}

fn ephemeral_identity_dir() -> Result<PathBuf, P2pClientError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = std::env::temp_dir().join(format!(
            "spendability-pir-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if let Err(error) =
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                    {
                        let _ = std::fs::remove_dir(&path);
                        return Err(P2pClientError::Start(error.to_string()));
                    }
                }
                return Ok(path);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(P2pClientError::Start(error.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ypir::{
        client::YPIRClient,
        params::{params_for_scenario_simplepir_with_config, YPIRSPConfig},
        serialize::ToBytes,
    };
    use zakura_network::zakura::framed_channel;

    #[test]
    fn ephemeral_identity_directories_are_unique() {
        let first = ephemeral_identity_dir().unwrap();
        let second = ephemeral_identity_dir().unwrap();
        assert_ne!(first, second);
        assert!(first.is_dir());
        assert!(second.is_dir());
        std::fs::remove_dir(first).unwrap();
        std::fs::remove_dir(second).unwrap();
    }

    #[test]
    fn current_queries_fit_message_limit() {
        for (rows, bits, poly_len) in [
            (
                nullifier_pir::NUM_BUCKETS as u64,
                (nullifier_pir::BUCKET_BYTES * 8) as u64,
                nullifier_pir::YPIR_POLY_LEN,
            ),
            (
                witness_pir::L0_DB_ROWS as u64,
                (witness_pir::SUBSHARD_ROW_BYTES * 8) as u64,
                witness_pir::YPIR_POLY_LEN,
            ),
        ] {
            let params = params_for_scenario_simplepir_with_config(
                rows,
                bits,
                YPIRSPConfig::for_poly_len(poly_len),
            );
            let (query, _) = YPIRClient::new(&params).generate_query_simplepir(0);
            assert!(query.to_bytes().len() <= MAX_MESSAGE_BYTES);
        }
    }

    #[tokio::test]
    async fn fragmented_request_and_response_round_trip() {
        let (request_send, mut request_recv) = framed_channel(256);
        let (response_send, response_recv) = framed_channel(256);
        let payload = vec![7; FRAME_PAYLOAD_BYTES + 1];
        let expected = payload.clone();
        tokio::spawn(async move {
            let mut decoder = MessageDecoder::default();
            loop {
                let frame = request_recv.recv().await.unwrap();
                if let Some((message, payload)) = decoder
                    .push(frame.message_type, frame.flags, frame.payload)
                    .unwrap()
                {
                    assert_eq!((message, payload), (Message::NullifierQueryReq, expected));
                    for frame in
                        frames(Message::NullifierQueryRes, vec![9; FRAME_PAYLOAD_BYTES + 1])
                    {
                        response_send.send(frame).await.unwrap();
                    }
                    break;
                }
            }
        });

        let mut response_recv = response_recv;
        assert_eq!(
            exchange(
                &mut response_recv,
                &request_send,
                Message::NullifierQueryReq,
                payload,
            )
            .await
            .unwrap(),
            vec![9; FRAME_PAYLOAD_BYTES + 1]
        );
    }

    #[tokio::test]
    async fn disconnect_ends_in_flight_request() {
        let (request_send, mut request_recv) = framed_channel(1);
        let (response_send, mut response_recv) = framed_channel(1);
        let request = tokio::spawn(async move {
            exchange(
                &mut response_recv,
                &request_send,
                Message::HealthReq,
                vec![],
            )
            .await
        });
        request_recv.recv().await.unwrap();
        drop(response_send);
        assert!(matches!(
            request.await.unwrap(),
            Err(P2pClientError::Disconnected)
        ));
    }
}
