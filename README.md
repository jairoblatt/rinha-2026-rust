# Rinha de Backend 2026 — Rust

Solução em Rust para a [Rinha de Backend 2026](https://github.com/zanfranceschi/rinha-de-backend-2026), uma competição de backend focada em detecção de fraude em transações de cartão usando busca por vizinhos mais próximos (k-NN) em vetores de alta dimensão.

## Arquitetura

```
cliente
  │
  ▼
Nginx (porta 9999)          0.10 CPU / 30 MB
  ├─ round-robin via UDS
  │
  ├──▶ instância 1 (Rust)   0.45 CPU / 160 MB
  └──▶ instância 2 (Rust)   0.45 CPU / 160 MB
```

A comunicação entre o Nginx e as instâncias usa **Unix Domain Sockets em tmpfs** (memória), eliminando o overhead de loopback TCP (~40–60µs por requisição no hardware da competição).

## Pipeline de detecção

```
requisição HTTP
      │
      ▼
  parser JSON customizado
      │
      ▼
  vetorização (14 dimensões → i16 quantizados)
      │
      ▼
  busca k-NN com AVX2 SIMD (5 vizinhos em 100k refs)
      │
      ▼
  contagem de fraudes nos vizinhos
      │
      ▼
  resposta pré-computada (bytes estáticos)
```

## Vetorização

Cada transação é convertida em um vetor de 14 dimensões conforme a especificação da competição:

| # | Dimensão | Fórmula |
|---|----------|---------|
| 0 | `amount` | `clamp(amount / 10_000)` |
| 1 | `installments` | `clamp(installments / 12)` |
| 2 | `amount_vs_avg` | `clamp((amount / avg_amount) / 10)` |
| 3 | `hour_of_day` | `hour / 23` |
| 4 | `day_of_week` | `day / 6` |
| 5 | `minutes_since_last_tx` | `clamp(minutes / 1_440)` ou `-1` se sem histórico |
| 6 | `km_from_last_tx` | `clamp(km / 1_000)` ou `-1` se sem histórico |
| 7 | `km_from_home` | `clamp(km / 1_000)` |
| 8 | `tx_count_24h` | `clamp(count / 20)` |
| 9 | `is_online` | `1` se online, `0` se presencial |
| 10 | `card_present` | `1` se cartão presente |
| 11 | `unknown_merchant` | `1` se comerciante desconhecido |
| 12 | `mcc_risk` | risco do MCC via lookup table |
| 13 | `merchant_avg_amount` | `clamp(avg / 10_000)` |

Os valores em ponto flutuante são quantizados para `i16` com escala 8192 (2¹³ bits de precisão fracional), reduzindo memória e acelerando o cálculo de distâncias com SIMD.

## Busca k-NN com AVX2

O dataset de referência (100.000 vetores × 16 dimensões × 2 bytes) é pré-processado em **build time**, convertido de `f32` para `i16` e embeddado diretamente no binário via `include_bytes!()`. Nenhum arquivo externo é lido em runtime.

A busca percorre os 100k vetores usando instruções **AVX2** para processar 16 valores `i16` simultaneamente:

- `_mm256_sub_epi16` — diferença elemento a elemento
- `_mm256_madd_epi16` — produto acumulado em pares, gerando `i32`
- Redução em árvore para somar os 8 acumuladores

A lista dos 5 mais próximos é mantida por insertion sort com **threshold pruning**: se a distância calculada já for maior que o 5º vizinho atual, o vetor é descartado sem entrar na lista.

## Otimizações de performance

### Parse JSON customizado
Nenhuma biblioteca de JSON é usada no caminho crítico. O parser manual usa `memchr::memmem` para localizar os campos por substring, extraindo apenas os valores necessários. Parsing de `f32` e timestamps ISO 8601 também são implementados à mão.

### Respostas pré-computadas
Só existem 6 resultados possíveis (0 a 5 vizinhos fraudulentos). As 6 strings JSON de resposta são `&'static [u8]` indexadas diretamente pelo contador, sem nenhuma alocação:

```
0 → {"approved":true,"fraud_score":0.0}
1 → {"approved":true,"fraud_score":0.2}
2 → {"approved":true,"fraud_score":0.4}
3 → {"approved":false,"fraud_score":0.6}
4 → {"approved":false,"fraud_score":0.8}
5 → {"approved":false,"fraud_score":1.0}
```

### Lookup tables
Valores discretos ou de baixa cardinalidade (`installments` 0–12, hora 0–23, dia 0–6, `tx_count_24h` 0–20) são quantizados via arrays estáticos, evitando divisões em ponto flutuante.

### Alinhamento de cache
- `AlignedRefs`: `align(64)` — alinhado à linha de cache L3
- `Query`: `align(32)` — alinhado para operações AVX2

### Compilação

O binário é compilado com `target-cpu=haswell` para habilitar AVX2, FMA e BMI2, e linkado estaticamente com musl, a imagem final usa `scratch` (zero camadas, zero dependências).

### Unix Sockets em tmpfs
O Nginx se comunica com as instâncias via sockets Unix montados em `/tmp/sockets` (tmpfs). Isso elimina o overhead de TCP loopback e a latência de sistema de arquivos.

## Stack

| Componente | Tecnologia |
|------------|------------|
| Runtime async | Tokio |
| HTTP server | Hyper 1.x |
| Busca de bytes | memchr |
| Decompressão (build) | flate2 |
| Load balancer | Nginx |
| Container | Docker / docker-compose |

## Como executar localmente

```bash
docker compose up --build
```

O serviço fica disponível em `http://localhost:9999`.

**Endpoints:**
- `GET /ready` — health check (retorna 200 quando pronto)
- `POST /fraud-score` — avalia uma transação

**Exemplo de requisição:**
```bash
curl -s -X POST http://localhost:9999/fraud-score \
  -H "Content-Type: application/json" \
  -d '{
    "id": "abc123",
    "transaction": { "amount": 250.0, "installments": 1, "requested_at": "2025-01-15T14:30:00Z" },
    "customer": { "avg_amount": 200.0, "tx_count_24h": 3, "known_merchants": ["mcid_1"] },
    "merchant": { "id": "mcid_1", "mcc": "5411", "avg_amount": 180.0 },
    "terminal": { "is_online": false, "card_present": true, "km_from_home": 2.5 },
    "last_transaction": { "timestamp": "2025-01-15T10:00:00Z", "km_from_current": 1.2 }
  }'
```

```json
{"approved":true,"fraud_score":0.0}
```
