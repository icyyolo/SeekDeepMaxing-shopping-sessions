# Shopping Sessions: native (Rust) pipeline, v3. v2 kept as Dockerfile.native.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY native3/Cargo.toml native3/Cargo.lock ./
COPY native3/src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
WORKDIR /app
CMD ["/app/sessions"]
