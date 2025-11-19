FROM rust:1.91-alpine AS chef
USER root
RUN apk add --no-cache musl-dev openssl-dev libressl libressl-dev pkgconfig perl make & cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin datalake-lambda

FROM alpine AS runtime
COPY --from=builder /app/target/release/datalake-lambda /bootstrap
ENTRYPOINT ["/bootstrap"]