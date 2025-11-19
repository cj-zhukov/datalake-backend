FROM rust:1.91-alpine AS chef
USER root
RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig build-base protobuf-dev
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/usr
ENV PKG_CONFIG_ALLOW_CROSS=1
RUN cargo build --release --bin datalake-fusion
# Also build Ballista binaries
RUN cargo install ballista-scheduler --version 49.0.0
RUN cargo install ballista-executor --version 49.0.0

FROM alpine AS runtime
RUN apk add --no-cache bash
WORKDIR /app

COPY --from=builder /app/target/release/datalake-fusion /usr/local/bin/
COPY --from=builder /usr/local/cargo/bin/ballista-scheduler /usr/local/bin/
COPY --from=builder /usr/local/cargo/bin/ballista-executor /usr/local/bin/

# Run both scheduler and executor for local test
ENTRYPOINT ["/bin/bash", "-c", "ballista-scheduler & ballista-executor & datalake-fusion"]
