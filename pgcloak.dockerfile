FROM rust:1.54.0 AS chef 
RUN cargo install cargo-chef 
WORKDIR app

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build -p pgcloak --release

FROM debian:buster-slim AS runtime
WORKDIR app
COPY --from=builder /app/target/release/pgcloak /usr/local/bin
ENTRYPOINT ["/usr/local/bin/app"]