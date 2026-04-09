# ADR-001: libmosquitto-Centric Message Delivery

**Status:** Accepted (Updated 2026-02-18)
**Date:** 2026-02-13
**Deciders:** @fsjobeck, @crhaglun

## Context

pg_mqtt_pub needs to publish MQTT messages from PostgreSQL triggers, SQL functions, and pg_cron jobs. The core tension is between **latency** (triggers should not slow down DML) and **durability** (messages should not be silently lost during broker outages).

Three approaches were considered:

1. **Ring buffer only** — shared memory, sub-millisecond, but volatile. Messages are lost on broker disconnect, Postgres crash, or restart.
2. **Outbox table only** — WAL-backed, crash-safe, but adds 2-10ms of write latency to every trigger fire due to the additional table INSERT and WAL fsync.
3. **Hybrid model with outbox** — ring buffer when healthy, outbox table during failures. Complex dual-path logic with mode transitions.

Previous implementation attempted approach #3 but proved over-engineered and did not reliably handle mode transitions. Attempting to manage retries and backoff manually was error-prone and duplicated functionality that libmosquitto already provides.

## Decision

We implement a **libmosquitto-centric architecture** that delegates all durability and retry logic to the MQTT client library, using MQTT's built-in QoS levels for ordering and delivery guarantees.

Messages are queued into a fast in-memory ring buffer and delivered via libmosquitto. The library handles message buffering, retries, reconnection, and acknowledgment tracking. Failures at the `mosquitto_publish()` call level (connection down, malformed topic) are immediately dead-lettered.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL Trigger / Function / pg_cron                   │
│  CALL mqtt_publish(topic, payload, qos, retain)            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  Shared Memory Ring Buffer      │
    │  • Fast, low-latency queue      │
    │  • ~17 MB default (1024 slots)  │
    │  • 16 KB max message size       │
    └────────────────┬────────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  Background Worker Process      │
    │  • Drain ring buffer in batches │
    │  • Call mosquitto_publish()     │
    │  • Track QoS acks (MQTT v5)     │
    │  • Handle dead letters          │
    └────────────────┬────────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  libmosquitto Client Library    │
    │  • Internal message buffer      │
    │  • QoS 1/2 delivery guarantees  │
    │  • Automatic reconnection       │
    │  • Persistent sessions          │
    │  • TLS/SASL support             │
    └────────────────┬────────────────┘
                     │
                     ▼
           ┌──────────────────┐
           │   MQTT Broker    │
           │  (mosquitto,     │
           │   HiveMQ, etc.)  │
           └──────────────────┘
```

### Message Delivery Flow

1. **Publish**: `mqtt_publish(topic, payload, qos, retain)` enqueues the message into the ring buffer.

2. **Worker drains**: Background worker pops messages in batches of 500 (configurable via `PGMQTTPUB_DRAIN_BATCH_SIZE`).

3. **libmosquitto delivery**:
   - **QoS 0** ("at most once"): Message is sent immediately. No guarantee of delivery.
   - **QoS 1** ("at least once"): libmosquitto waits for PUBACK from broker before considering it sent. Persists in client buffer across reconnects.
   - **QoS 2** ("exactly once"): libmosquitto waits for full PUBCOMP handshake. Highest durability.

4. **Acknowledgment**: For QoS 1/2, libmosquitto's callback (`on_publish_v5`) fires when the broker ACKs. Worker tracks this and removes the message from in-flight tracking.

5. **Failure handling**: If `mosquitto_publish()` fails immediately (connection down, malformed topic, out of memory), the message is inserted into `mqtt_pub.dead_letters` with the mosquitto error code.

### Message Ordering

Message ordering is preserved by libmosquitto's client-side queue, which delivers messages in the order they were submitted (subject to QoS guarantees).

- Within a single QoS level, messages are delivered in FIFO order
- QoS 0 messages may be delivered out of order relative to QoS 1/2 if they overlap
- Persistent sessions ensure messages survive broker restarts (QoS 1/2 only)

### Dead Letters

Immediate publish failures are recorded in `mqtt_pub.dead_letters` with:
- `topic` and `payload` for identification
- `qos` and `retain` flags
- `mosquitto_error_code` — the numeric error from libmosquitto
- `error_message` — human-readable error description
- `failed_at` — timestamp of failure

Common reasons for dead-letter entries:
- **Connection down at publish time** — broker is unreachable (typically transient; libmosquitto will retry later)
- **Malformed topic** — contains null bytes or characters invalid per MQTT spec
- **Payload too large** — exceeds 16 KB limit
- **Out of memory in libmosquitto** — rare; indicates system stress

## Configuration

| GUC | Default | Notes |
|-----|---------|-------|
| `pg_mqtt_pub.queue_size` | 1024 | Ring buffer slots (64–1,048,576); power of 2 recommended |
| `pg_mqtt_pub.broker_host` | `localhost` | MQTT broker hostname or IP |
| `pg_mqtt_pub.broker_port` | 1883 | MQTT port (1883 plain, 8883 TLS) |
| `pg_mqtt_pub.broker_username` | (empty) | Authentication username |
| `pg_mqtt_pub.broker_password` | (empty) | Authentication password |
| `pg_mqtt_pub.broker_use_tls` | `false` | Enable TLS encryption |
| `pg_mqtt_pub.broker_ca_cert` | (empty) | Path to CA certificate (PEM) |
| `pg_mqtt_pub.broker_client_cert` | (empty) | Path to client cert (mutual TLS) |
| `pg_mqtt_pub.broker_client_key` | (empty) | Path to client key (mutual TLS) |
| `pg_mqtt_pub.init_database` | `postgres` | Database for worker SPI operations |

## Consequences

### Benefits
- **Simple architecture** — no dual-path mode switching logic; libmosquitto handles all durability
- **Sub-millisecond latency** — ring buffer enqueue is ~0.1ms; no outbox table INSERT penalty during normal operation
- **Message ordering preserved** — libmosquitto queues guarantee FIFO within QoS levels
- **No trigger failures** — publish() succeeds or raises an error; DML is never blocked
- **Automatic reconnection** — libmosquitto handles broker outages and reconnection with exponential backoff
- **Persistent sessions** — QoS 1/2 messages survive broker/client restarts when broker supports persistence
- **Observable** — `mqtt_status()` shows connection state, message counters, and queue depth

### Trade-offs
- **Volatile during outages** — QoS 0 messages are lost if the broker is unreachable. Triggers continue but messages don't survive.
- **Broker connection required for QoS 1/2 delivery** — messages queue in libmosquitto's client buffer, but broker must be reachable for final delivery.
- **No PostgreSQL-level durability** — messages don't write to WAL. A Postgres crash after `mqtt_publish()` returns but before the worker publishes could lose messages (in-flight window).
- **Dead letters are minimal** — only immediate publish failures are captured; later failures during libmosquitto's retry loop are invisible (acceptable; libmosquitto owns retry logic).
- **No per-broker routing** — single broker connection. Multiple brokers would require multiple extension instances or custom application logic.

### Rationale for This Design Over Hot/Cold Hybrid

The original hot/cold hybrid model had these issues:

1. **Mode transition complexity** — the logic to detect when to switch modes and ensure ordering was fragile and error-prone.
2. **Duplicated retry logic** — we were implementing exponential backoff and dead-lettering, which libmosquitto already does well via QoS levels.
3. **Over-engineered for the common case** — most deployments have reliable brokers; the outbox table optimization for outages added code without benefit.
4. **Testing burden** — mode transitions required complex test scenarios to validate.

The libmosquitto-centric approach:
- Delegates durability to a proven, battle-tested MQTT client library
- Eliminates the need for mode switching logic
- Reduces code by ~1000 lines
- Achieves similar latency characteristics for the common case

**Trade-off**: We lose WAL-backed durability for messages in flight. This is acceptable because:
- QoS 1/2 messages are persisted by the broker (if broker supports persistence)
- QoS 0 is best-effort; users should not rely on it for critical messages
- Most PostgreSQL deployments can tolerate message loss during catastrophic failures (Postgres crash + lost in-flight)

### Not Addressed (Future Work)
- **Per-broker message routing** — currently supports single broker. Multiple brokers would require application-level routing or separate extension instances.
- **Exactly-once delivery** — not guaranteed end-to-end. QoS 2 provides exactly-once at the protocol level, but replay from dead letters could cause duplicates. Consumers should be idempotent.
- **Configurable batch size** — drain batch size (500) is compile-time constant. Could expose as GUC if deployments need tuning.

## References

- [MQTT QoS Levels](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [libmosquitto C Library](https://mosquitto.org/api/c/)
- [PostgreSQL Background Workers](https://www.postgresql.org/docs/current/bgworker.html)
- [PostgreSQL Shared Memory](https://www.postgresql.org/docs/current/spi.html)
