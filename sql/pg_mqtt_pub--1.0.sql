/* pg_mqtt_pub--1.0.sql — Extension SQL definitions */

\echo Use "CREATE EXTENSION pg_mqtt_pub" to load this file. \quit

-- ═══════════════════════════════════════════
--  Outbox Table (Cold Path — WAL-durable)
--
--  Written to when broker is disconnected or
--  ring buffer is full. Background worker
--  drains this FIFO before resuming hot path.
-- ═══════════════════════════════════════════

CREATE TABLE mqtt_pub.outbox (
    id              bigserial       PRIMARY KEY,
    broker_name     text            NOT NULL DEFAULT 'default',
    topic           text            NOT NULL,
    payload         bytea           NOT NULL,
    qos             smallint        NOT NULL DEFAULT 0
                                    CHECK (qos BETWEEN 0 AND 2),
    retain          boolean         NOT NULL DEFAULT false,
    attempts        smallint        NOT NULL DEFAULT 0,
    next_retry_at   timestamptz     NOT NULL DEFAULT now(),
    created_at      timestamptz     NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE mqtt_pub.outbox IS
    'Durable message queue for MQTT publish when brokers are unavailable. '
    'Drained FIFO by the background worker on reconnection.';

-- Retry scheduling index (skip rows not yet eligible)
CREATE INDEX outbox_retry_idx
    ON mqtt_pub.outbox (next_retry_at)
    WHERE attempts > 0;

-- ═══════════════════════════════════════════
--  Dead Letters Table
--
--  Messages that exceeded max delivery attempts.
--  Retained for investigation, auto-pruned by
--  the background worker.
-- ═══════════════════════════════════════════

CREATE TABLE mqtt_pub.dead_letters (
    id              bigserial       PRIMARY KEY,
    original_id     bigint,
    broker_name     text            NOT NULL,
    topic           text            NOT NULL,
    payload         bytea           NOT NULL,
    qos             smallint        NOT NULL,
    retain          boolean         NOT NULL DEFAULT false,
    attempts        smallint        NOT NULL,
    first_failed_at timestamptz,
    last_error      text,
    dead_lettered_at timestamptz    NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE mqtt_pub.dead_letters IS
    'Messages that failed to publish after max attempts. '
    'Auto-pruned after pg_mqtt_pub.dead_letter_retain_days. '
    'Inspect with: SELECT * FROM mqtt_pub.dead_letters ORDER BY dead_lettered_at DESC;';

CREATE INDEX dead_letters_prune_idx
    ON mqtt_pub.dead_letters (dead_lettered_at);

-- ═══════════════════════════════════════════
--  Core Publish Functions
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_publish(
    topic    text,
    payload  text    DEFAULT '',
    qos      integer DEFAULT 0,
    retain   boolean DEFAULT false,
    broker   text    DEFAULT 'default'
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_publish'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION mqtt_publish IS
    'Publish a text payload to an MQTT topic. '
    'Routes through ring buffer (hot) or outbox table (cold) automatically.';

CREATE FUNCTION mqtt_publish_json(
    topic    text,
    data     jsonb,
    qos      integer DEFAULT 0,
    retain   boolean DEFAULT false,
    broker   text    DEFAULT 'default'
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_publish_json'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION mqtt_publish_json IS
    'Publish a JSONB value as JSON string to an MQTT topic.';

CREATE FUNCTION mqtt_publish_batch(
    topic_prefix text,
    payloads     text[],
    qos          integer DEFAULT 0,
    retain       boolean DEFAULT false,
    broker       text    DEFAULT 'default'
)
RETURNS integer
AS 'MODULE_PATHNAME', 'mqtt_publish_batch'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION mqtt_publish_batch IS
    'Publish an array of payloads as individual MQTT messages. Returns count accepted.';

-- ═══════════════════════════════════════════
--  Broker Management
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_broker_add(
    name     text,
    host     text,
    port     integer  DEFAULT 1883,
    username text     DEFAULT NULL,
    password text     DEFAULT NULL,
    use_tls  boolean  DEFAULT false,
    ca_cert  text     DEFAULT NULL
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_broker_add'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION mqtt_broker_add IS
    'Register a named MQTT broker connection.';

CREATE FUNCTION mqtt_broker_remove(name text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_broker_remove'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;

COMMENT ON FUNCTION mqtt_broker_remove IS
    'Remove a named broker. Cannot remove "default".';

-- ═══════════════════════════════════════════
--  Status / Monitoring
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_status(
    OUT broker_name      text,
    OUT host             text,
    OUT port             integer,
    OUT connected        boolean,
    OUT delivery_mode    text,
    OUT messages_sent    bigint,
    OUT messages_failed  bigint,
    OUT dead_lettered    bigint,
    OUT outbox_pending   bigint,
    OUT last_error       text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'mqtt_status'
LANGUAGE C VOLATILE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION mqtt_status IS
    'Connection status, delivery mode (hot/cold), and queue depths for all brokers.';

-- ═══════════════════════════════════════════
--  Monitoring Views
-- ═══════════════════════════════════════════

CREATE VIEW mqtt_pub.outbox_summary AS
SELECT
    broker_name,
    count(*)                                    AS pending,
    count(*) FILTER (WHERE attempts > 0)        AS retrying,
    max(attempts)                               AS max_attempts_seen,
    min(created_at)                             AS oldest_message,
    max(created_at)                             AS newest_message
FROM mqtt_pub.outbox
GROUP BY broker_name;

COMMENT ON VIEW mqtt_pub.outbox_summary IS
    'Aggregate view of outbox state per broker.';

CREATE VIEW mqtt_pub.dead_letter_summary AS
SELECT
    broker_name,
    count(*)                                    AS total,
    min(dead_lettered_at)                       AS oldest,
    max(dead_lettered_at)                       AS newest,
    array_agg(DISTINCT left(last_error, 80))    AS error_types
FROM mqtt_pub.dead_letters
GROUP BY broker_name;

COMMENT ON VIEW mqtt_pub.dead_letter_summary IS
    'Summary of dead-lettered messages per broker.';

-- ═══════════════════════════════════════════
--  Generic Trigger Function
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_trigger_notify()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
DECLARE
    _topic_prefix text;
    _qos          integer;
    _broker       text;
    _include_meta boolean;
    _topic        text;
    _payload      jsonb;
    _data         jsonb;
    _result       boolean;
BEGIN
    _topic_prefix := TG_ARGV[0];
    _qos          := COALESCE(TG_ARGV[1]::integer, 1);
    _broker       := COALESCE(TG_ARGV[2], 'default');
    _include_meta := COALESCE(TG_ARGV[3]::boolean, true);

    _topic := _topic_prefix || '/' || lower(TG_OP);

    IF TG_OP = 'DELETE' THEN
        _data := row_to_json(OLD)::jsonb;
    ELSIF TG_OP = 'UPDATE' THEN
        _data := jsonb_build_object(
            'old', row_to_json(OLD)::jsonb,
            'new', row_to_json(NEW)::jsonb
        );
    ELSE
        _data := row_to_json(NEW)::jsonb;
    END IF;

    IF _include_meta THEN
        _payload := jsonb_build_object(
            '_operation', TG_OP,
            '_table',     TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
            '_timestamp', now(),
            '_txid',      txid_current(),
            'data',       _data
        );
    ELSE
        _payload := _data;
    END IF;

    _result := mqtt_pub.mqtt_publish(_topic, _payload::text, _qos, false, _broker);

    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$func$;

COMMENT ON FUNCTION mqtt_trigger_notify IS
    'Generic trigger: publishes row changes as MQTT messages. '
    'Args: topic_prefix, qos (default 1), broker (default ''default''), include_meta (default true).';

-- ═══════════════════════════════════════════
--  Resultset Trigger Function
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_trigger_notify_resultset()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
DECLARE
    _topic_prefix text;
    _qos          integer;
    _broker       text;
    _include_meta boolean;
    _topic        text;
    _payload      jsonb;
    _data         jsonb;
    _count        integer;
    _result       boolean;
BEGIN
    _topic_prefix := TG_ARGV[0];
    _qos          := COALESCE(TG_ARGV[1]::integer, 1);
    _broker       := COALESCE(TG_ARGV[2], 'default');
    _include_meta := COALESCE(TG_ARGV[3]::boolean, true);

    _topic := _topic_prefix || '/' || lower(TG_OP);

    IF TG_OP = 'DELETE' THEN
        -- Aggregate all deleted rows into JSON array
        SELECT jsonb_agg(row_to_json(d)::jsonb)
        INTO _data
        FROM old_table d;

    ELSIF TG_OP = 'UPDATE' THEN
        -- Correlate old and new rows using row_number()
        WITH old_rows AS (
            SELECT row_number() OVER () as rn, row_to_json(o)::jsonb as data
            FROM old_table o
        ),
        new_rows AS (
            SELECT row_number() OVER () as rn, row_to_json(n)::jsonb as data
            FROM new_table n
        )
        SELECT jsonb_agg(
            jsonb_build_object('old', o.data, 'new', n.data)
        )
        INTO _data
        FROM old_rows o
        JOIN new_rows n ON o.rn = n.rn;

    ELSE -- INSERT
        SELECT jsonb_agg(row_to_json(n)::jsonb)
        INTO _data
        FROM new_table n;
    END IF;

    -- Skip publishing if no rows were affected
    IF _data IS NULL THEN
        RETURN NULL;
    END IF;

    _count := jsonb_array_length(_data);

    IF _include_meta THEN
        _payload := jsonb_build_object(
            '_operation', TG_OP,
            '_table',     TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
            '_timestamp', now(),
            '_txid',      txid_current(),
            '_count',     _count,
            'data',       _data
        );
    ELSE
        _payload := _data;
    END IF;

    _result := mqtt_pub.mqtt_publish(_topic, _payload::text, _qos, false, _broker);

    RETURN NULL;
END;
$func$;

COMMENT ON FUNCTION mqtt_trigger_notify_resultset IS
    'Statement-level trigger: publishes all affected rows as a single MQTT message. '
    'Args: topic_prefix, qos (default 1), broker (default ''default''), include_meta (default true).';

-- ═══════════════════════════════════════════
--  Trigger Setup Helpers
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_trigger_event_setup(
    table_name   text,
    topic_prefix text    DEFAULT NULL,
    operations   text[]  DEFAULT '{INSERT,UPDATE,DELETE}',
    qos          integer DEFAULT 1,
    broker       text    DEFAULT 'default',
    include_meta boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    _ops          text;
    _prefix       text;
    _trigger_name text;
    _sql          text;
BEGIN
    _prefix := COALESCE(topic_prefix, 'db/changes/' || table_name);
    _ops := array_to_string(operations, ' OR ');
    _trigger_name := 'mqtt_auto_' || replace(table_name, '.', '_');

    _sql := format(
        'CREATE OR REPLACE TRIGGER %I
             AFTER %s ON %s
             FOR EACH ROW
             EXECUTE FUNCTION mqtt_pub.mqtt_trigger_notify(%L, %L, %L, %L)',
        _trigger_name, _ops, table_name,
        _prefix, qos::text, broker, include_meta::text
    );

    EXECUTE _sql;

    RAISE NOTICE 'pg_mqtt_pub: trigger "%" created on % for %',
                 _trigger_name, table_name, _ops;
END;
$func$;

COMMENT ON FUNCTION mqtt_trigger_event_setup IS
    'One-liner to create an event-based (row-level) MQTT publish trigger on a table.';

CREATE FUNCTION mqtt_trigger_resultset_setup(
    table_name   text,
    topic_prefix text    DEFAULT NULL,
    operations   text[]  DEFAULT '{INSERT,UPDATE,DELETE}',
    qos          integer DEFAULT 1,
    broker       text    DEFAULT 'default',
    include_meta boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    _prefix       text;
    _trigger_name text;
    _referencing  text;
    _sql          text;
    _op           text;
    _count        integer := 0;
BEGIN
    _prefix := COALESCE(topic_prefix, 'db/changes/' || table_name);

    -- Create separate trigger for each operation (required for transition tables)
    FOREACH _op IN ARRAY operations LOOP
        _trigger_name := 'mqtt_resultset_' || replace(table_name, '.', '_') || '_' || lower(_op);

        -- Set REFERENCING clause based on operation
        CASE _op
            WHEN 'INSERT' THEN
                _referencing := ' REFERENCING NEW TABLE AS new_table';
            WHEN 'DELETE' THEN
                _referencing := ' REFERENCING OLD TABLE AS old_table';
            WHEN 'UPDATE' THEN
                _referencing := ' REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table';
            ELSE
                RAISE EXCEPTION 'Invalid operation: %. Must be INSERT, UPDATE, or DELETE', _op;
        END CASE;

        _sql := format(
            'CREATE OR REPLACE TRIGGER %I
                 AFTER %s ON %s%s
                 FOR EACH STATEMENT
                 EXECUTE FUNCTION mqtt_pub.mqtt_trigger_notify_resultset(%L, %L, %L, %L)',
            _trigger_name, _op, table_name, _referencing,
            _prefix, qos::text, broker, include_meta::text
        );

        EXECUTE _sql;
        _count := _count + 1;

        RAISE NOTICE 'pg_mqtt_pub: resultset trigger "%" created for %',
                     _trigger_name, _op;
    END LOOP;

    IF _count = 0 THEN
        RAISE EXCEPTION 'No operations specified. operations array cannot be empty.';
    END IF;
END;
$func$;

COMMENT ON FUNCTION mqtt_trigger_resultset_setup IS
    'One-liner to create a resultset-based (statement-level) MQTT publish trigger on a table. '
    'Publishes all affected rows in a single message per statement.';

-- ═══════════════════════════════════════════
--  Dead Letter Replay Helper
-- ═══════════════════════════════════════════

CREATE FUNCTION mqtt_pub.replay_dead_letters(
    p_broker_name text DEFAULT NULL,
    p_limit       integer DEFAULT 100
)
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
    _row    record;
    _count  integer := 0;
BEGIN
    FOR _row IN
        SELECT id, broker_name, topic, payload, qos, retain
        FROM mqtt_pub.dead_letters
        WHERE (p_broker_name IS NULL OR broker_name = p_broker_name)
        ORDER BY id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    LOOP
        -- Re-insert into outbox for another attempt
        INSERT INTO mqtt_pub.outbox (broker_name, topic, payload, qos, retain, attempts)
        VALUES (_row.broker_name, _row.topic, _row.payload, _row.qos, _row.retain, 0);

        DELETE FROM mqtt_pub.dead_letters WHERE id = _row.id;
        _count := _count + 1;
    END LOOP;

    RETURN _count;
END;
$func$;

COMMENT ON FUNCTION mqtt_pub.replay_dead_letters IS
    'Move dead-lettered messages back to the outbox for retry. '
    'Optionally filter by broker name and limit count.';

-- ═══════════════════════════════════════════
--  Schema Permissions
-- ═══════════════════════════════════════════

GRANT USAGE ON SCHEMA mqtt_pub TO PUBLIC;
GRANT SELECT ON mqtt_pub.outbox TO PUBLIC;
GRANT SELECT ON mqtt_pub.dead_letters TO PUBLIC;
GRANT SELECT ON mqtt_pub.outbox_summary TO PUBLIC;
GRANT SELECT ON mqtt_pub.dead_letter_summary TO PUBLIC;

GRANT EXECUTE ON FUNCTION mqtt_publish(text, text, integer, boolean, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_publish_json(text, jsonb, integer, boolean, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_publish_batch(text, text[], integer, boolean, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_broker_add(text, text, integer, text, text, boolean, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_broker_remove(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_trigger_notify() TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_trigger_notify_resultset() TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_trigger_event_setup(text, text, text[], integer, text, boolean) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_trigger_resultset_setup(text, text, text[], integer, text, boolean) TO PUBLIC;
GRANT EXECUTE ON FUNCTION mqtt_pub.replay_dead_letters(text, integer) TO PUBLIC;
