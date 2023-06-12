.PHONY: fmt
fmt: ## Format all the Rust code.
	cargo fmt --all

.PHONY: check
check: ## Cargo check all the targets.
	cargo check --workspace --all-targets

.PHONY: clippy
clippy: ## Check clippy rules.
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: fmt-toml
fmt-toml: ## Format all TOML files.
	taplo format --option "indent_string=    "

.PHONY: clean
clean: ## Clean the project.
	cargo clean
