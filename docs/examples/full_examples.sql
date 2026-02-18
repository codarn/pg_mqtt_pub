-- ═══════════════════════════════════════════════════════════════
--  pg_mqtt_pub Examples — Ring Buffer with libmosquitto Buffering
-- ═══════════════════════════════════════════════════════════════
--
--  Architecture:
--  - Ring buffer for inter-process transport (volatile, fast)
--  - libmosquitto handles QoS 1/2 with automatic retries
--  - Immediate dead-lettering on mosquitto_publish() failures
--
--  NOTE: Broker configuration is set in postgresql.conf (requires restart):
--    pg_mqtt_pub.broker_host = 'localhost'
--    pg_mqtt_pub.broker_port = 1883
--    pg_mqtt_pub.broker_username = ''      # optional
--    pg_mqtt_pub.broker_password = ''      # optional
--    pg_mqtt_pub.broker_use_tls = false    # optional
--    pg_mqtt_pub.broker_ca_cert = ''       # optional
-- ═══════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS pg_mqtt_pub;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- ───────────────────────────────────
-- 1. Check broker status
-- ───────────────────────────────────

SELECT * FROM mqtt_status();

-- ───────────────────────────────────
-- 2. Basic trigger setup (publish on each row change)
-- ───────────────────────────────────

CREATE TABLE sensor_readings (
    id          serial PRIMARY KEY,
    sensor_id   text NOT NULL,
    value       double precision NOT NULL,
    unit        text DEFAULT 'celsius',
    location    text,
    recorded_at timestamptz DEFAULT now()
);

-- ───────────────────────────────────
-- 3. Threshold alert with QoS 2
-- ───────────────────────────────────

CREATE OR REPLACE FUNCTION alert_high_temperature()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.value > 40.0 THEN
        -- Publish alert with QoS 2 (higher reliability)
        PERFORM mqtt_publish(
            format('alerts/temperature/%s', NEW.sensor_id),  -- topic
            json_build_object(
                'sensor_id', NEW.sensor_id,
                'value',     NEW.value,
                'severity',  CASE
                    WHEN NEW.value > 60 THEN 'critical'
                    WHEN NEW.value > 50 THEN 'warning'
                    ELSE 'info'
                END,
                'location',  NEW.location,
                'timestamp', NEW.recorded_at
            )::text,  -- payload
            2,        -- qos
            true      -- retain
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sensor_alert
    AFTER INSERT ON sensor_readings
    FOR EACH ROW EXECUTE FUNCTION alert_high_temperature();

-- ───────────────────────────────────
-- 4. pg_cron: periodic analytics
-- ───────────────────────────────────

SELECT cron.schedule('mqtt-hourly-stats', '0 * * * *', $$
    SELECT mqtt_publish(
        'analytics/temperature/hourly',  -- topic
        (
            SELECT json_build_object(
                'period_start', date_trunc('hour', now() - interval '1 hour'),
                'period_end',   date_trunc('hour', now()),
                'sensors', json_agg(json_build_object(
                    'sensor_id', sensor_id,
                    'avg', round(avg_val::numeric, 2),
                    'min', min_val, 'max', max_val, 'count', cnt
                ))
            )::text
            FROM (
                SELECT sensor_id, avg(value) avg_val, min(value) min_val,
                       max(value) max_val, count(*) cnt
                FROM sensor_readings
                WHERE recorded_at >= date_trunc('hour', now() - interval '1 hour')
                  AND recorded_at <  date_trunc('hour', now())
                GROUP BY sensor_id
            ) agg
        ),  -- payload
        1,   -- qos
        true -- retain
    );
$$);

-- ───────────────────────────────────
-- 5. pg_cron: batch sync with dead letter awareness
-- ───────────────────────────────────

CREATE TABLE orders (
    id             serial PRIMARY KEY,
    customer_id    integer NOT NULL,
    total          numeric(10,2),
    status         text DEFAULT 'pending',
    created_at     timestamptz DEFAULT now(),
    mqtt_synced    boolean DEFAULT false,
    mqtt_synced_at timestamptz
);

SELECT cron.schedule('mqtt-sync-orders', '*/5 * * * *', $$
    WITH batch AS (
        SELECT id, row_to_json(o)::text as payload
        FROM orders o
        WHERE NOT mqtt_synced
        ORDER BY created_at
        LIMIT 500
        FOR UPDATE SKIP LOCKED
    ),
    sent AS (
        SELECT b.id,
               mqtt_publish(
                   'erp/orders/' || b.id,  -- topic
                   b.payload,  -- payload
                   2           -- qos (higher reliability for order sync)
               ) as ok
        FROM batch b
    )
    UPDATE orders SET mqtt_synced = true, mqtt_synced_at = now()
    WHERE id IN (SELECT id FROM sent WHERE ok);
$$);

-- ───────────────────────────────────
-- 6. Monitor system
-- ───────────────────────────────────

-- Broker status dashboard
SELECT * FROM mqtt_status();

-- Connection uptime (how long connected)
SELECT
    host, port, connected,
    CASE
        WHEN connected THEN
            'Connected for ' || EXTRACT(EPOCH FROM (now() - connected_since))::integer || ' seconds'
        WHEN disconnected_since IS NOT NULL THEN
            'Disconnected for ' || EXTRACT(EPOCH FROM (now() - disconnected_since))::integer || ' seconds'
        ELSE 'Never connected'
    END as uptime_status
FROM mqtt_status();

-- Message throughput and success rate
SELECT
    host, port,
    messages_sent, messages_failed, dead_lettered,
    ROUND(100.0 * messages_sent / NULLIF(messages_sent + messages_failed, 0), 2) as success_rate_pct,
    queue_depth
FROM mqtt_status();

-- Dead letter summary
SELECT * FROM mqtt_pub.dead_letter_summary;

-- Dead letter investigation (with mosquitto error codes)
SELECT id, topic, mosquitto_error_code, error_message, failed_at
FROM mqtt_pub.dead_letters
ORDER BY failed_at DESC
LIMIT 20;

-- ───────────────────────────────────
-- 7. pg_cron: self-monitoring
-- ───────────────────────────────────

SELECT cron.schedule('mqtt-health-check', '* * * * *', $cron$
    DO $code$
    DECLARE
        r record;
    BEGIN
        FOR r IN SELECT * FROM mqtt_status() LOOP
            -- Alert on broker disconnection
            IF NOT r.connected THEN
                RAISE WARNING 'pg_mqtt_pub: broker at %:%  disconnected since %',
                    r.host, r.port, r.disconnected_since;
            END IF;
            -- Alert on dead lettered messages
            IF r.dead_lettered > 0 THEN
                RAISE WARNING 'pg_mqtt_pub: % dead-lettered messages (%.2f%% failure rate)',
                    r.dead_lettered,
                    100.0 * r.messages_failed / NULLIF(r.messages_sent + r.messages_failed, 0);
            END IF;
            -- Alert on queue buildup
            IF r.queue_depth > 100 THEN
                RAISE WARNING 'pg_mqtt_pub: queue depth at % (worker may be slow)',
                    r.queue_depth;
            END IF;
        END LOOP;
    END $code$;
$cron$);
