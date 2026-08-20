use super::state::AppState;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use nullifier_pir::{PirEngine, ServerPhase};
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
pub struct HealthResponse {
    pub phase: ServerPhase,
    pub height: Option<u64>,
    pub nullifier_count: u64,
    pub num_blocks: u64,
}

pub async fn health<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
) -> Json<HealthResponse> {
    let phase = (**state.phase.load()).clone();
    let pir = state.live_pir.load();
    let (height, nullifier_count, num_blocks) = match pir.as_ref() {
        Some(pir_state) => (
            Some(pir_state.metadata.latest_height),
            pir_state.metadata.num_nullifiers,
            pir_state.metadata.num_buckets,
        ),
        None => (None, 0, 0),
    };
    Json(HealthResponse {
        phase,
        height,
        nullifier_count,
        num_blocks,
    })
}

pub async fn metadata<P: PirEngine + 'static>(State(state): State<Arc<AppState<P>>>) -> Response {
    let pir = state.live_pir.load();
    match pir.as_ref() {
        Some(pir_state) => Json(&pir_state.metadata).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

pub async fn params<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
) -> Json<nullifier_pir::YpirScenario> {
    Json(state.scenario.clone())
}

pub async fn query<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
    body: Bytes,
) -> Response {
    let pir = state.live_pir.load();
    let pir_state = match pir.as_ref() {
        Some(s) => s,
        None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    match state.engine.answer_query(&pir_state.engine_state, &body) {
        Ok(response_bytes) => response_bytes.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "PIR query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
