/* pg_mqtt_pub--1.0.sql — Extension SQL definitions */

\echo Use "CREATE EXTENSION pg_mqtt_pub" to load this file. \quit

-- ═══════════════════════════════════════════
--  Dead Letters Table
--
--  Messages that failed to publish immediately
--  when calling mosquitto_publish().
--  Retained for investigation, auto-pruned by
--  the background worker.
-- ═══════════════════════════════════════════

CREATE TABLE mqtt_pub.dead_letters (
    id                      bigserial       PRIMARY KEY,
    topic                   text            NOT NULL,
    payload                 text            NOT NULL,
    qos                     smallint        NOT NULL,
    retain                  boolean         NOT NULL DEFAULT false,
    mosquitto_error_code    integer         NOT NULL,
    error_message           text            NOT NULL,
    failed_at               timestamptz     NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE mqtt_pub.dead_letters IS
    'Messages that failed immediately during mosquitto_publish(). '
    'Includes mosquitto error code and message for debugging. '
    'Auto-pruned after pg_mqtt_pub.dead_letter_retain_days. '
    'Inspect with: SELECT * FROM mqtt_pub.dead_letters ORDER BY failed_at DESC;';

CREATE INDEX dead_letters_prune_idx
    ON mqtt_pub.dead_letters (failed_at);

-- ═══════════════════════════════════════════
--  Core Publish Functions
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_publish(
    topic    text,
    payload  text    DEFAULT '',
    qos      integer DEFAULT 0,
    retain   boolean DEFAULT false
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_publish'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION mqtt_publish IS
    'Publish a text payload to an MQTT topic via ring buffer. '
    'Returns false if ring buffer is full. Messages are queued for async delivery via background worker. '
    'Ring buffer messages are NOT rollback-safe: if your transaction rolls back after mqtt_publish(), '
    'the message may still be delivered. Use AFTER COMMIT triggers for transactional guarantees.';

-- ═══════════════════════════════════════════
--  Status / Monitoring
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_status(
    OUT host              text,
    OUT port              integer,
    OUT connected         boolean,
    OUT messages_sent     bigint,
    OUT messages_failed   bigint,
    OUT dead_lettered     bigint,
    OUT queue_depth       integer,
    OUT connected_since   timestamptz,
    OUT disconnected_since timestamptz,
    OUT worker_pid        integer
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'mqtt_status'
LANGUAGE C STABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION mqtt_status IS
    'Real-time broker connection metrics and message counters. '
    'Returns host, port, connection state, message counters, queue depth, '
    'connection timestamps, and worker process ID.';

-- ═══════════════════════════════════════════
--  Monitoring Views
-- ═══════════════════════════════════════════

CREATE VIEW mqtt_pub.dead_letter_summary AS
SELECT
    count(*)                                        AS total,
    min(failed_at)                                  AS oldest,
    max(failed_at)                                  AS newest,
    array_agg(DISTINCT left(error_message, 80))    AS error_types
FROM mqtt_pub.dead_letters;

COMMENT ON VIEW mqtt_pub.dead_letter_summary IS
    'Summary of dead-lettered messages, grouped by error type.';

-- ═══════════════════════════════════════════
--  Schema Permissions
-- ═══════════════════════════════════════════

GRANT USAGE ON SCHEMA mqtt_pub TO PUBLIC;
GRANT SELECT ON mqtt_pub.dead_letters TO PUBLIC;
GRANT SELECT ON mqtt_pub.dead_letter_summary TO PUBLIC;

GRANT EXECUTE ON FUNCTION mqtt_publish(text, text, integer, boolean) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_status() TO PUBLIC;
