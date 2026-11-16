# Public image: pocketbi/agent
# Build: docker build -t pocketbi/agent .
# Run:   docker run --rm -e POCKETBI_TOKEN=… -e POCKETBI_URL=wss://… pocketbi/agent

FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin agent

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/agent /usr/local/bin/agent
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/agent"]
