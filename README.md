# pg_mqtt_pub — PostgreSQL MQTT Publish Extension

A PostgreSQL extension that publishes MQTT messages directly from SQL, triggers, and `pg_cron` jobs. Messages are queued in a fast in-memory ring buffer and delivered via libmosquitto, which handles all durability, retries, and reconnection logic using standard MQTT QoS levels.

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                               │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  pg_trigger   │  │   pg_cron    │  │  SELECT mqtt_publish │  │
│  │  AFTER INSERT │  │  */5 * * * * │  │  (topic, payload)    │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
│         └──────────────────┼────────────────────┘              │
│                            ▼                                    │
│                  ┌───────────────────┐                          │
│                  │  mqtt_publish()    │                          │
│                  │  Ring Buffer       │                          │
│                  │  (shared memory)   │                          │
│                  └─────────┬─────────┘                          │
│                            │                                    │
│                            ▼                                    │
│              ┌──────────────────────────┐                       │
│              │  Background Worker        │                       │
│              │  • Drain ring buffer      │                       │
│              │  • Call mosquitto_publish │                       │
│              │  • Handle QoS acks        │                       │
│              │  • Manage dead letters    │                       │
│              └────────────┬─────────────┘                       │
│                           │                                     │
│                  ┌────────▼──────────┐                          │
│                  │  libmosquitto     │                          │
│                  │  • Internal buffer│                          │
│                  │  • Retries (QoS)  │                          │
│                  │  • Reconnection   │                          │
│                  │  • TLS support    │                          │
│                  └────────┬──────────┘                          │
│                           │                                     │
└───────────────────────────┼─────────────────────────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │   MQTT Broker    │
                  │   (mosquitto,    │
                  │    HiveMQ, etc.) │
                  └──────────────────┘
```

### Message Flow

1. **Publish** — `mqtt_publish(topic, payload, qos, retain)` enqueues the message into the in-memory ring buffer.
2. **Worker drains** — Background worker pops messages from the ring buffer in batches (500 messages at a time).
3. **libmosquitto handles delivery** — For QoS 0, the message is sent immediately. For QoS 1/2, libmosquitto buffers it and waits for broker acknowledgment using persistent sessions.
4. **Failures are dead-lettered** — If `mosquitto_publish()` fails immediately (connection down, malformed topic), the message is moved to `mqtt_pub.dead_letters` for investigation.
5. **Message ordering** — libmosquitto preserves message order within the client queue, so messages are delivered in the order they were published (subject to QoS guarantees).

## Building

### Dependencies

```bash
# Ubuntu/Debian
sudo apt install postgresql-server-dev-17 libmosquitto-dev

# RHEL/Fedora
sudo dnf install postgresql17-devel mosquitto-devel

# Arch
sudo pacman -S postgresql-libs mosquitto
```

### Compile & Install

```bash
make
sudo make install
```

### Enable

```ini
# postgresql.conf
shared_preload_libraries = 'pg_mqtt_pub'
```

```sql
CREATE EXTENSION pg_mqtt_pub;
```

## Configuration (postgresql.conf)

All parameters are required to be set at server startup (PGC_POSTMASTER) and must be added to `postgresql.conf`.

### Broker Connection (Required)

```ini
# Broker host and port
pg_mqtt_pub.broker_host = 'localhost'            # Hostname or IP address
pg_mqtt_pub.broker_port = 1883                   # MQTT port (1883 for plain, 8883 for TLS)

# Authentication (optional)
pg_mqtt_pub.broker_username = ''                 # Leave empty for anonymous access
pg_mqtt_pub.broker_password = ''                 # Only used if username is set

# TLS/SSL (optional)
pg_mqtt_pub.broker_use_tls = false               # Enable TLS encryption
pg_mqtt_pub.broker_ca_cert = ''                  # Path to CA certificate (PEM format)
pg_mqtt_pub.broker_client_cert = ''              # Path to client certificate for mutual TLS
pg_mqtt_pub.broker_client_key = ''               # Path to client private key for mutual TLS
```

### Optional Configuration

```ini
pg_mqtt_pub.queue_size = 1024                    # Ring buffer capacity in slots (64-1048576)
pg_mqtt_pub.init_database = 'postgres'           # Database where pg_mqtt_pub extension is installed
                                                  # Worker connects to this database for dead_letters table
                                                  # MUST match the database where you ran CREATE EXTENSION
                                                  # (default: postgres)
```

**Queue Size Notes:**
- Each slot holds a message up to 16 KB (topic + payload)
- Default 1024 slots = ~17 MB shared memory
- Increase if you're publishing many messages per second (e.g., from triggers firing in bulk)
- Recommended: power of 2 for efficiency (64, 128, 256, 512, 1024, 2048, ...)

## SQL API

### Publishing

```sql
-- Publish text payload
SELECT mqtt_publish(
    topic   := 'sensors/temp/room1',
    payload := '{"value": 23.5}',
    qos     := 1,
    retain  := false
);
```

### Monitoring

```sql
-- Broker connection status and message metrics
SELECT * FROM mqtt_status();

-- Example output:
--  host      | port | connected | messages_sent | messages_failed | dead_lettered | queue_depth | connected_since | disconnected_since | worker_pid
-- -----------+------+-----------+---------------+-----------------+---------------+-------------+-----------------+--------------------+----------
--  localhost | 1883 | true      |         12847 |               3 |             1 |           0 | 2026-02-18 09:15:00 | (null)             | 1234

-- Dead letter diagnostics
SELECT * FROM mqtt_pub.dead_letters ORDER BY failed_at DESC;

-- Summary view
SELECT * FROM mqtt_pub.dead_letter_summary;
```

#### Replay failed messages

Re-publish dead-lettered messages after fixing the underlying issue:

```sql
-- Re-publish the oldest 50 dead letters
DO $$
DECLARE
    _row record;
    _count integer := 0;
BEGIN
    FOR _row IN
        SELECT id, topic, payload, qos, retain
        FROM mqtt_pub.dead_letters
        ORDER BY failed_at
        LIMIT 50
        FOR UPDATE SKIP LOCKED
    LOOP
        -- Re-publish via mqtt_publish()
        PERFORM mqtt_publish(_row.topic, _row.payload, _row.qos, _row.retain);

        -- Clean up on success
        DELETE FROM mqtt_pub.dead_letters WHERE id = _row.id;
        _count := _count + 1;
    END LOOP;

    RAISE NOTICE 'Replayed % dead letters', _count;
END $$;
```

### Stress Testing

Use the `stress_test_mqtt()` function to validate throughput, latency, and reliability under load. This function generates and publishes sequential test messages to the `stress/test` topic.

```sql
-- Basic: publish 1000 messages as fast as possible
SELECT * FROM stress_test_mqtt(1000);

-- With batching: publish 1000 messages in batches of 100 with 10ms delay between batches
SELECT * FROM stress_test_mqtt(
    message_count := 1000,
    batch_size := 100,
    delay_ms := 10,
    qos := 1
);

-- High volume: 10000 messages at QoS 1 with minimal throttling
SELECT * FROM stress_test_mqtt(
    message_count := 10000,
    batch_size := 500,
    delay_ms := 1,
    qos := 1
);
```

**Parameters:**
- `message_count` — Total number of test messages to publish
- `batch_size` — Publish this many messages before delaying (default: 1 = no batching)
- `delay_ms` — Milliseconds to wait between batches (default: 1)
- `qos` — MQTT QoS level: 0 (fire-and-forget), 1 (at-least-once), or 2 (exactly-once) (default: 0)

**Returns:** Table with `msg_num` (message sequence number) and `result` (true if published successfully). Monitor results to detect publish failures:

```sql
-- Check for failed messages
SELECT msg_num, result FROM stress_test_mqtt(1000) WHERE result = false;

-- Summary of success rate
SELECT
    count(*) as total,
    count(*) FILTER (WHERE result = true) as successful,
    count(*) FILTER (WHERE result = false) as failed,
    round(100.0 * count(*) FILTER (WHERE result = true) / count(*), 2) as success_rate_pct
FROM stress_test_mqtt(1000);
```

**Monitor the broker connection during stress testing:**

```sql
-- In another session, watch status and dead letters in real-time
SELECT
    (SELECT * FROM mqtt_status()) as status,
    (SELECT count(*) FROM mqtt_pub.dead_letters) as dead_letter_count;
```

### Triggers

#### Row-level trigger for specific updates

Track order status changes and publish to MQTT:

```sql
CREATE FUNCTION notify_order_status_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Only publish if status actually changed
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        PERFORM mqtt_publish(
            topic   := format('orders/status/%s', NEW.order_id),
            payload := json_build_object(
                'order_id', NEW.order_id,
                'old_status', OLD.status,
                'new_status', NEW.status,
                'changed_at', now()
            )::text,
            qos     := 1,
            retain  := false
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER order_status_change
    AFTER UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION notify_order_status_change();
```

#### Row-level trigger for all changes

Publish every insert/update/delete to a change feed:

```sql
CREATE FUNCTION publish_row_changes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    _payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        _payload := row_to_json(OLD)::jsonb;
    ELSIF TG_OP = 'UPDATE' THEN
        _payload := jsonb_build_object(
            'old', row_to_json(OLD)::jsonb,
            'new', row_to_json(NEW)::jsonb
        );
    ELSE
        _payload := row_to_json(NEW)::jsonb;
    END IF;

    PERFORM mqtt_publish(
        topic   := format('changes/%s/%s', TG_TABLE_NAME, lower(TG_OP)),
        payload := _payload::text,
        qos     := 0
    );

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER publish_changes
    AFTER INSERT OR UPDATE OR DELETE ON sensor_readings
    FOR EACH ROW
    EXECUTE FUNCTION publish_row_changes();
```

### Custom Trigger with Filtering

```sql
CREATE FUNCTION alert_on_high_temp() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.value > 40.0 THEN
        PERFORM mqtt_publish(
            topic   := format('alerts/temp/%s', NEW.sensor_id),
            payload := json_build_object(
                'sensor_id', NEW.sensor_id,
                'value', NEW.value,
                'severity', CASE WHEN NEW.value > 60 THEN 'critical' ELSE 'warning' END
            )::text,
            qos := 2, retain := true
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER temp_alert AFTER INSERT ON sensor_readings
    FOR EACH ROW EXECUTE FUNCTION alert_on_high_temp();
```

### pg_cron Integration

```sql
-- Hourly aggregation
SELECT cron.schedule('mqtt-hourly-stats', '0 * * * *', $$
    SELECT mqtt_publish(
        topic := 'analytics/temperature/hourly',
        payload := (SELECT json_build_object(
            'hour', date_trunc('hour', now() - interval '1 hour'),
            'avg',  round(avg(value)::numeric, 2),
            'count', count(*)
        )::text FROM sensor_readings
        WHERE recorded_at >= date_trunc('hour', now() - interval '1 hour')),
        qos := 1, retain := true
    );
$$);

-- Health heartbeat every 30s
SELECT cron.schedule('mqtt-heartbeat', '*/30 * * * * *', $$
    SELECT mqtt_publish(
        topic := 'monitoring/postgres/heartbeat',
        payload := json_build_object(
            'timestamp', now(),
            'active_conns', (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
            'db_size_mb', pg_database_size(current_database()) / 1048576
        )::text,
        qos := 0, retain := true
    );
$$);
```

## Schema

The extension creates the `mqtt_pub` schema containing:

| Object | Type | Purpose |
|--------|------|---------|
| `mqtt_pub.dead_letters` | table | Messages that failed to publish immediately |
| `mqtt_pub.dead_letter_summary` | view | Summary of dead letter messages and error types |

**Dead Letters**: When `mosquitto_publish()` fails immediately (before being added to libmosquitto's queue), the message is recorded in `dead_letters` with the mosquitto error code and message. This can happen if:
- The broker connection is down and cannot be established
- The topic is malformed (contains invalid characters per MQTT spec)
- The payload is too large (>16 KB)
- Out of memory in libmosquitto

Dead letters are retained for investigation and can be manually re-published via `mqtt_publish()` after fixing the underlying issue.

## Troubleshooting

### "relation mqtt_pub.dead_letters does not exist"

This error indicates a configuration mismatch between where the extension is installed and where the worker connects.

**Diagnosis:**

Check which database has the extension installed:
```sql
SELECT datname,
       (SELECT count(*) FROM pg_extension WHERE extname = 'pg_mqtt_pub') as has_extension
FROM pg_database;
```

Check the worker's configured database:
```sql
SHOW pg_mqtt_pub.init_database;
```

**Solution:**

Ensure `pg_mqtt_pub.init_database` in `postgresql.conf` matches the database where you ran `CREATE EXTENSION pg_mqtt_pub`.

If you need to fix it:
```sql
-- In the database specified by pg_mqtt_pub.init_database
DROP EXTENSION IF EXISTS pg_mqtt_pub CASCADE;
CREATE EXTENSION pg_mqtt_pub;
```

Then restart PostgreSQL:
```bash
systemctl restart postgresql
# or if using Docker:
docker compose restart postgres
```

### Worker Not Starting

Check PostgreSQL logs for detailed error messages:
```bash
# View recent logs
tail -f /var/log/postgresql/postgresql.log

# or if using Docker
docker compose logs postgres | grep "pg_mqtt_pub"
```

Common issues:
- Missing `pg_mqtt_pub` in `shared_preload_libraries` in `postgresql.conf`
- Incorrect `pg_mqtt_pub.init_database` setting (see above)
- Extension not installed in the init_database (see above)
- Broker connection issues - check `pg_mqtt_pub.broker_host` and `pg_mqtt_pub.broker_port` are correct

## Docker Quick Start

```bash
docker compose up -d

# Watch MQTT messages in real-time
docker compose logs -f mqtt-monitor

# Connect to Postgres
docker compose exec postgres psql -U postgres -d testdb

# Try it
CREATE EXTENSION pg_mqtt_pub;
SELECT mqtt_publish('test/hello', 'world');
SELECT * FROM mqtt_status();
```

## Architecture Decision Records

Design decisions are documented in [`docs/adr/`](docs/adr/):

- [ADR-001: Hybrid Delivery Model](docs/adr/001-hybrid-delivery-model.md) — Hot/cold path design, poison message guardrails, mode transition logic

## License

PostgreSQL License (same as PostgreSQL itself).
