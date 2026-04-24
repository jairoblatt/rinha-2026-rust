FROM --platform=linux/amd64 rust:1.83-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src

COPY Cargo.toml ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
    cargo fetch && rm -rf src

COPY build.rs ./
COPY src ./src
COPY resources ./resources

ENV RUSTFLAGS="-C target-cpu=haswell -C target-feature=+avx2,+fma,+f16c,+bmi2,+popcnt -C link-arg=-s"
RUN cargo build --release --target x86_64-unknown-linux-musl && \
    strip target/x86_64-unknown-linux-musl/release/rinha-fraud-2026

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/rinha-fraud-2026 /rinha
ENTRYPOINT ["/rinha"]
