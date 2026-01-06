.PHONY: all
all:
	@echo "Run my targets individually!"

.PHONY: clean
clean:
	cargo clean && rm -rf target && rm -f python/kafkars/*.so

.PHONY: env
env:
	test -d .venv || uv sync --python=3.12
	. .venv/bin/activate && \
		uv sync --group=dev


.PHONY: develop
develop: env
	. .venv/bin/activate && uv run maturin develop

.PHONY: test
test: develop
	RUST_BACKTRACE=1 uv run python -m pytest python/tests && . .venv/bin/activate && cargo test

.PHONY: build
build: env
	uv run maturin build

.PHONY: dist
dist: env
	. .venv/bin/activate && \
		docker run --rm -v $(shell pwd):/io ghcr.io/pyo3/maturin build --release --strip --out dist

.PHONY: lint
lint:
	. .venv/bin/activate && \
		cargo fmt && \
		cargo clippy -- -D warnings && \
		pre-commit run --all-files

.PHONY: coverage-env
coverage-env:
	cargo install grcov
	rustup component add llvm-tools

.PHONY: coverage
coverage: develop coverage-env
	uv run coverage run --source=python/kafkars -m pytest python/tests
	uv run coverage xml -o coverage.xml
	. .venv/bin/activate && CARGO_INCREMENTAL=0 RUSTFLAGS="-Cinstrument-coverage" LLVM_PROFILE_FILE="kafkars-%p-%m.profraw"  cargo test
	grcov . -s . --binary-path ./target/debug/ -t lcov --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" -o lcov.info

.PHONY: update
update:
	cargo generate-lockfile
	uv lock --upgrade
	pre-commit autoupdate && pre-commit run --all-files
	# uv pip compile docs/requirements.txt.in > docs/requirements.txt

.PHONY: generate-ci
generate-ci: develop
	uv run maturin generate-ci github --output=.github/workflows/ci.yaml --platform=manylinux --platform=macos

.PHONY: docs
docs:
	uv run --group=docs mkdocs serve

.PHONY: docs-build
docs-build:
	uv run --group=docs mkdocs build --strict
