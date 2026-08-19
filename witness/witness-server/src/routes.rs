use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use pir_types::{PirEngine, ServerPhase};
use serde::Serialize;
use std::sync::Arc;
use witness_types::FrontierRange;

#[derive(Serialize)]
pub struct HealthResponse {
    pub phase: ServerPhase,
    pub anchor_height: Option<u64>,
    pub tree_size: u64,
    pub populated_shards: u32,
}

pub async fn health<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
) -> Json<HealthResponse> {
    let phase = (**state.phase.load()).clone();
    let pir = state.live_pir.load();
    let (anchor_height, tree_size, populated_shards) = match pir.as_ref() {
        Some(ps) => (
            Some(ps.metadata.anchor_height),
            ps.metadata.tree_size,
            ps.metadata.populated_shards,
        ),
        None => (None, 0, 0),
    };
    Json(HealthResponse {
        phase,
        anchor_height,
        tree_size,
        populated_shards,
    })
}

pub async fn metadata<P: PirEngine + 'static>(State(state): State<Arc<AppState<P>>>) -> Response {
    let pir = state.live_pir.load();
    match pir.as_ref() {
        Some(ps) => Json(&ps.metadata).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

pub async fn broadcast<P: PirEngine + 'static>(State(state): State<Arc<AppState<P>>>) -> Response {
    let pir = state.live_pir.load();
    match pir.as_ref() {
        Some(ps) => Json(&ps.broadcast).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

pub async fn frontier<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
    Query(range): Query<FrontierRange>,
) -> Response {
    match state.frontier_range(range.from, range.to) {
        Some(updates) => Json(updates).into_response(),
        None if range.from > range.to => StatusCode::BAD_REQUEST.into_response(),
        None => StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
    }
}

pub async fn params<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
) -> Json<pir_types::YpirScenario> {
    Json(state.scenario.clone())
}

pub async fn query<P: PirEngine + 'static>(
    State(state): State<Arc<AppState<P>>>,
    body: Bytes,
) -> Response {
    let t0 = std::time::Instant::now();
    let pir = state.live_pir.load();
    let pir_state = match pir.as_ref() {
        Some(s) => s,
        None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    let query_bytes = body.len();
    match state.engine.answer_query(&pir_state.engine_state, &body) {
        Ok(response_bytes) => {
            tracing::info!(
                elapsed_ms = t0.elapsed().as_millis(),
                query_bytes,
                response_bytes = response_bytes.len(),
                "witness query answered",
            );
            response_bytes.into_response()
        }
        Err(e) => {
            tracing::error!(elapsed_ms = t0.elapsed().as_millis(), error = %e, "PIR query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
