FROM --platform=linux/amd64 rust:1.83-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src

COPY Cargo.toml ./
RUN mkdir -p src/bin && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'fn main() {}' > src/bin/build_index.rs && \
    cargo fetch && rm -rf src

COPY build.rs ./
COPY src ./src
COPY resources ./resources

ENV RUSTFLAGS="-C target-cpu=haswell -C target-feature=+avx2,+fma,+f16c,+bmi2,+popcnt -C link-arg=-s"

COPY data/index.bin.gz* ./data/
RUN if [ ! -f data/index.bin.gz ]; then \
        cargo build --release --target x86_64-unknown-linux-musl --bin build_index && \
        ./target/x86_64-unknown-linux-musl/release/build_index; \
    fi

RUN cargo build --release --target x86_64-unknown-linux-musl --bin rinha-fraud-2026 && \
    strip target/x86_64-unknown-linux-musl/release/rinha-fraud-2026

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/rinha-fraud-2026 /rinha
ENTRYPOINT ["/rinha"]
