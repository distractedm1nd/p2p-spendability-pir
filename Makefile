# PIR Server
# Top-level Makefile for local development
#
# Single binary: spend-server (combined nullifier + witness PIR)
# Feature flags: --features nullifier, --features witness, or both (default)
#
# Usage: make build && make run

# ── Configuration ────────────────────────────────────────────────────
DATA_DIR  ?= ./data
LISTEN    ?= 0.0.0.0:8080
LWD_URL   := https://us.zec.stardust.rest:443
ZAKURA_CONFIG ?= ./zakura.toml

# ── Targets ──────────────────────────────────────────────────────────

.PHONY: build build-nullifier build-witness \
        run run-nullifier run-witness \
        test test-ypir clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build spend-server with both nullifier + witness (default)
	cargo build --release -p combined-server

build-nullifier: ## Build spend-server with nullifier only
	cargo build --release -p combined-server --no-default-features --features nullifier

build-witness: ## Build spend-server with witness only
	cargo build --release -p combined-server --no-default-features --features witness

run: ## Run combined HTTP + Zakura P2P server with both subsystems
	cargo run --release -p combined-server -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN) \
		--zakura-config $(ZAKURA_CONFIG)

run-nullifier: ## Run spend-server with nullifier only
	cargo run --release -p combined-server --no-default-features --features nullifier -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN)

run-witness: ## Run spend-server with witness only
	cargo run --release -p combined-server --no-default-features --features witness -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN)

test: ## Run workspace tests
	cargo test --workspace

test-ypir: ## Run all tests including YPIR round-trips (release mode)
	cargo test --workspace --all-features --release

clean: ## Remove build artifacts and data files
	cargo clean
	rm -rf $(DATA_DIR)
