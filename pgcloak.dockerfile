FROM rust:slim-buster AS chef
RUN cargo install cargo-chef

WORKDIR app

## PLANNER
FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

## BUILDER
FROM chef AS builder

RUN apt update \
    && apt-get -y install libssl-dev pkg-config

COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json

COPY . .

RUN cargo build -p pgcloak --release

## RUNTIME
FROM debian:buster-slim AS runtime

RUN apt update \
    && apt-get -y install libssl-dev pkg-config

WORKDIR /app

COPY --from=builder /app/target/release/pgcloak /usr/local/bin

EXPOSE 6432
ENTRYPOINT ["/usr/local/bin/pgcloak"]
