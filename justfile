# Default recipe
default:
    @echo "Run recipes individually! Use 'just --list' to see available recipes."

# Remove all build artifacts
clean:
    cargo clean
    rm -rf target
    find . -name "*.profraw" | xargs rm -f
    rm -f python/kafkars/*.so
    rm -rf site
    rm -f .coverage
    rm -f coverage.xml
    rm -f lcov.info
    rm -rf .venv

# Create virtual environment and install dependencies
env:
    test -d .venv || uv sync --python=3.12
    . .venv/bin/activate && uv sync --group=dev

# Build the Python extension in development mode
develop: env
    . .venv/bin/activate && uv run maturin develop

# Run all tests
test: develop
    RUST_BACKTRACE=1 uv run python -m pytest python/tests && . .venv/bin/activate && cargo test

# Build the Python package
build: env
    uv run maturin build

# Build distributable wheels via Docker
dist: env
    . .venv/bin/activate && docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release --strip --out dist

# Format and lint
lint:
    . .venv/bin/activate && cargo fmt && cargo clippy --all-targets -- -D warnings && prek run --all-files

# Install coverage tools
coverage-env:
    cargo install grcov
    rustup component add llvm-tools

# Run coverage for both Python and Rust
coverage: develop coverage-env
    uv run coverage run --source=python/kafkars -m pytest python/tests && \
        uv run coverage xml -o coverage.xml
    . .venv/bin/activate && CARGO_INCREMENTAL=0 RUSTFLAGS="-Cinstrument-coverage" LLVM_PROFILE_FILE="kafkars-%p-%m.profraw" cargo test
    grcov . -s . --binary-path ./target/debug/ -t lcov --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" -o lcov.info
    @echo ""
    @echo "=== Python Coverage Summary ==="
    uv run coverage report
    @echo ""
    @echo "=== Rust Coverage Summary ==="
    grcov . -s . --binary-path ./target/debug/ -t markdown --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" --ignore "/*/.cargo/*" --ignore "/*/.rustup/*"

# Update dependencies
update:
    cargo generate-lockfile
    uv lock --upgrade
    prek autoupdate
    -prek run --all-files
    prek run --all-files

# Update dependencies and create a PR branch
update-e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [ "$branch" != "master" ]; then
        echo "Error: must be on master branch (currently on '$branch')"
        exit 1
    fi
    if [ -n "$(git diff --stat)" ]; then
        echo "Error: working directory has uncommitted changes"
        exit 1
    fi
    just update
    if [ -n "$(git diff --stat)" ]; then
        date=$(date +%Y-%m-%d)
        git checkout -b "update-$date"
        git add -A
        git commit -m "Update $date"
        git push -u origin "update-$date"
    else
        echo "No changes after update"
    fi

# Generate CI release workflow
generate-ci: develop
    uv run maturin generate-ci github --output=.github/workflows/ci.yaml --platform=manylinux --platform=macos

# Serve documentation locally
docs:
    uv run --group=docs mkdocs serve

# Build documentation (strict mode)
docs-build:
    uv run --group=docs mkdocs build --strict
