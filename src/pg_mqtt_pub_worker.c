/*
 * pg_mqtt_pub_worker.c — MQTT Background Worker (Hybrid Delivery)
 *
 * Main loop priority:
 *   1. Drain outbox table (cold path recovery, FIFO order preserved)
 *   2. Drain ring buffer (hot path, lowest latency)
 *
 * On broker disconnect:
 *   - Sets delivery_mode → COLD (routes new messages to outbox)
 *   - Attempts reconnection with exponential backoff
 *
 * On broker reconnect:
 *   - Drains ALL outbox rows first (preserving order from outage)
 *   - Only then sets delivery_mode → HOT
 *   - Resumes ring buffer consumption
 *
 * Poison message guardrails:
 *   - Each outbox row tracks `attempts` count
 *   - On publish failure, `attempts` is incremented with exponential next_retry_at
 *   - At max attempts (GUC configurable), row moves to mqtt_pub.dead_letters
 *   - Dead letters are retained for configurable days, then pruned
 *
 * Copyright (c) 2025, PostgreSQL License
 */

#include "postgres.h"

#include "pg_mqtt_pub.h"

#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include <mosquitto.h>
#include <signal.h>

/* ───────── Per-Broker Connection Handle ───────── */

typedef struct BrokerHandle
{
    struct mosquitto   *mosq;
    int                 broker_idx;
    TimestampTz         last_reconnect;
    int                 reconnect_backoff_ms;
    bool                connected;
} BrokerHandle;

/* ───────── Worker-local State ───────── */

static volatile sig_atomic_t got_sighup  = false;
static volatile sig_atomic_t got_sigterm = false;

static BrokerHandle handles[PGMQTTPUB_MAX_BROKERS];
static int          num_handles = 0;

/* ───────── Signal Handlers ───────── */

static void
pgmqttpub_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void
pgmqttpub_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* ───────── Delivery Mode Transitions ───────── */

static void
switch_to_cold_mode(void)
{
    uint32 current = PGMQTTPUB_MODE_HOT;

    if (pg_atomic_compare_exchange_u32(&pgmqttpub_shared->delivery_mode,
                                        &current, PGMQTTPUB_MODE_COLD))
    {
        pgmqttpub_shared->mode_changed_at = GetCurrentTimestamp();
        elog(LOG, "pg_mqtt_pub: switched to COLD mode (outbox table)");
    }
}

static void
switch_to_hot_mode(void)
{
    uint32 current = PGMQTTPUB_MODE_COLD;

    if (pg_atomic_compare_exchange_u32(&pgmqttpub_shared->delivery_mode,
                                        &current, PGMQTTPUB_MODE_HOT))
    {
        pgmqttpub_shared->mode_changed_at = GetCurrentTimestamp();
        elog(LOG, "pg_mqtt_pub: switched to HOT mode (ring buffer)");
    }
}

/* ───────── Mosquitto Callbacks ───────── */

static void
on_connect(struct mosquitto *mosq, void *userdata, int reason_code)
{
    BrokerHandle      *h = (BrokerHandle *)userdata;
    PgMqttPubBrokerState *bs;

    if (!pgmqttpub_shared)
        return;

    bs = &pgmqttpub_shared->broker_states[h->broker_idx];

    if (reason_code == 0)
    {
        h->connected = true;
        h->reconnect_backoff_ms = pgmqttpub_reconnect_interval_ms;
        bs->state = PGMQTTPUB_CONN_CONNECTED;
        bs->connected_since = GetCurrentTimestamp();
        bs->last_error[0] = '\0';

        elog(LOG, "pg_mqtt_pub: connected to broker '%s' (%s:%d)",
             pgmqttpub_shared->brokers[h->broker_idx].name,
             pgmqttpub_shared->brokers[h->broker_idx].host,
             pgmqttpub_shared->brokers[h->broker_idx].port);

        /* NOTE: do NOT switch to HOT here — outbox must be drained first.
         * The main loop handles the HOT transition after outbox is empty. */
    }
    else
    {
        h->connected = false;
        bs->state = PGMQTTPUB_CONN_ERROR;
        snprintf(bs->last_error, sizeof(bs->last_error),
                 "Connection refused: %s", mosquitto_connack_string(reason_code));

        elog(WARNING, "pg_mqtt_pub: broker '%s' refused: %s",
             pgmqttpub_shared->brokers[h->broker_idx].name,
             mosquitto_connack_string(reason_code));
    }
}

static void
on_disconnect(struct mosquitto *mosq, void *userdata, int reason_code)
{
    BrokerHandle      *h = (BrokerHandle *)userdata;
    PgMqttPubBrokerState *bs;

    if (!pgmqttpub_shared)
        return;

    bs = &pgmqttpub_shared->broker_states[h->broker_idx];
    h->connected = false;
    bs->state = PGMQTTPUB_CONN_DISCONNECTED;
    bs->disconnected_since = GetCurrentTimestamp();

    /* Immediately switch to COLD mode so new messages go to outbox */
    switch_to_cold_mode();

    if (reason_code != 0)
    {
        snprintf(bs->last_error, sizeof(bs->last_error),
                 "Unexpected disconnect: rc=%d", reason_code);
        elog(WARNING, "pg_mqtt_pub: broker '%s' disconnected (rc=%d) — switched to COLD mode",
             pgmqttpub_shared->brokers[h->broker_idx].name, reason_code);
    }
}

static void
on_publish(struct mosquitto *mosq, void *userdata, int mid)
{
    /* QoS 1/2 ack received — could track in-flight count here */
}

/* ───────── Connection Management ───────── */

static void
setup_broker_connection(int idx)
{
    PgMqttPubBrokerConfig *bc;
    BrokerHandle          *h;
    char                   client_id[128];

    bc = &pgmqttpub_shared->brokers[idx];
    h  = &handles[num_handles];

    memset(h, 0, sizeof(BrokerHandle));
    h->broker_idx = idx;
    h->reconnect_backoff_ms = pgmqttpub_reconnect_interval_ms;

    snprintf(client_id, sizeof(client_id), "pg_mqtt_pub_%s_%d",
             bc->name, MyProcPid);

    h->mosq = mosquitto_new(client_id, true, h);
    if (!h->mosq)
    {
        elog(WARNING, "pg_mqtt_pub: failed to create mosquitto instance for '%s'",
             bc->name);
        return;
    }

    mosquitto_connect_callback_set(h->mosq, on_connect);
    mosquitto_disconnect_callback_set(h->mosq, on_disconnect);
    mosquitto_publish_callback_set(h->mosq, on_publish);

    if (bc->username[0] != '\0')
        mosquitto_username_pw_set(h->mosq, bc->username,
                                  bc->password[0] ? bc->password : NULL);

    if (bc->use_tls)
    {
        int rc = mosquitto_tls_set(h->mosq,
                                    bc->ca_cert_path[0] ? bc->ca_cert_path : NULL,
                                    NULL,
                                    bc->client_cert_path[0] ? bc->client_cert_path : NULL,
                                    bc->client_key_path[0] ? bc->client_key_path : NULL,
                                    NULL);
        if (rc != MOSQ_ERR_SUCCESS)
            elog(WARNING, "pg_mqtt_pub: TLS setup failed for '%s': %s",
                 bc->name, mosquitto_strerror(rc));

        mosquitto_tls_opts_set(h->mosq, 1, NULL, NULL);
    }

    mosquitto_threaded_set(h->mosq, true);
    num_handles++;
}

static void
try_connect(BrokerHandle *h)
{
    PgMqttPubBrokerConfig *bc;
    PgMqttPubBrokerState  *bs;
    int                    rc;
    TimestampTz            now;

    bc = &pgmqttpub_shared->brokers[h->broker_idx];
    bs = &pgmqttpub_shared->broker_states[h->broker_idx];
    now = GetCurrentTimestamp();

    if (h->last_reconnect != 0)
    {
        long secs;
        int microsecs;
        TimestampDifference(h->last_reconnect, now, &secs, &microsecs);

        if ((secs * 1000 + microsecs / 1000) < h->reconnect_backoff_ms)
            return;
    }

    h->last_reconnect = now;
    bs->state = PGMQTTPUB_CONN_CONNECTING;

    elog(LOG, "pg_mqtt_pub: connecting to '%s' at %s:%d",
         bc->name, bc->host, bc->port);

    rc = mosquitto_connect_async(h->mosq, bc->host, bc->port, 60);
    if (rc != MOSQ_ERR_SUCCESS)
    {
        bs->state = PGMQTTPUB_CONN_ERROR;
        snprintf(bs->last_error, sizeof(bs->last_error),
                 "Connect failed: %s", mosquitto_strerror(rc));
        h->reconnect_backoff_ms = Min(h->reconnect_backoff_ms * 2, 60000);

        elog(WARNING, "pg_mqtt_pub: connect to '%s' failed: %s (retry in %dms)",
             bc->name, mosquitto_strerror(rc), h->reconnect_backoff_ms);
    }
}

static void
refresh_broker_connections(void)
{
    int i;
    int j;
    bool found;

    LWLockAcquire(pgmqttpub_shared->config_lock, LW_SHARED);

    for (i = 0; i < PGMQTTPUB_MAX_BROKERS; i++)
    {
        if (!pgmqttpub_shared->brokers[i].active)
            continue;

        found = false;
        for (j = 0; j < num_handles; j++)
        {
            if (handles[j].broker_idx == i)
            { found = true; break; }
        }

        if (!found && num_handles < PGMQTTPUB_MAX_BROKERS)
        {
            LWLockRelease(pgmqttpub_shared->config_lock);
            setup_broker_connection(i);
            LWLockAcquire(pgmqttpub_shared->config_lock, LW_SHARED);
        }
    }

    LWLockRelease(pgmqttpub_shared->config_lock);
}

/* ───────── Find Broker Handle by Name ───────── */

static BrokerHandle *
find_handle_for_broker(const char *name)
{
    int i;
    for (i = 0; i < num_handles; i++)
    {
        PgMqttPubBrokerConfig *bc = &pgmqttpub_shared->brokers[handles[i].broker_idx];
        if (strcmp(bc->name, name) == 0)
            return &handles[i];
    }
    return NULL;
}

/* ───────── Publish to MQTT Broker ───────── */

static bool
publish_to_broker(const char *broker_name, const char *topic,
                  const void *payload, int payload_len,
                  int qos, bool retain)
{
    BrokerHandle      *h;
    PgMqttPubBrokerState *bs;
    int                rc;

    h = find_handle_for_broker(broker_name);
    if (!h)
    {
        elog(WARNING, "pg_mqtt_pub: no broker '%s' found, message dropped", broker_name);
        return false;
    }

    bs = &pgmqttpub_shared->broker_states[h->broker_idx];

    if (!h->connected)
    {
        bs->messages_failed++;
        return false;
    }

    rc = mosquitto_publish(h->mosq, NULL, topic, payload_len, payload, qos, retain);

    if (rc == MOSQ_ERR_SUCCESS)
    {
        bs->messages_sent++;
        return true;
    }

    bs->messages_failed++;
    snprintf(bs->last_error, sizeof(bs->last_error),
             "Publish failed: %s", mosquitto_strerror(rc));
    return false;
}

/* ───────── Dispatch Ring Buffer Message ───────── */

static bool
dispatch_ringbuf_message(PgMqttPubMessage *msg)
{
    char topic[PGMQTTPUB_MAX_TOPIC_LEN + 1];
    int  qos    = msg->flags & PGMQTTPUB_FLAG_QOS_MASK;
    bool retain = (msg->flags & PGMQTTPUB_FLAG_RETAIN) != 0;

    memcpy(topic, msg->data, msg->topic_len);
    topic[msg->topic_len] = '\0';

    return publish_to_broker(msg->broker_name, topic,
                             msg->data + msg->topic_len,
                             msg->payload_len, qos, retain);
}

/* ═══════════════════════════════════════════
 *  Outbox Drain — the core of the cold path
 *
 *  SELECT id, broker_name, topic, payload, qos, retain, attempts
 *  FROM mqtt_pub.outbox
 *  WHERE next_retry_at <= now()
 *  ORDER BY id
 *  LIMIT $batch_size
 *  FOR UPDATE SKIP LOCKED;
 *
 *  For each row:
 *    - Try publish to MQTT
 *    - Success → DELETE row, decrement outbox_pending
 *    - Failure:
 *        - attempts < max → UPDATE attempts++, set next_retry_at with backoff
 *        - attempts >= max → MOVE to dead_letters, log warning
 *
 *  Returns: number of rows successfully published
 * ═══════════════════════════════════════════ */

static int
drain_outbox(void)
{
    int  ret;
    int  published = 0;
    int  processed;
    int  i;
    char query[512];

    /* Collect message info BEFORE any DML to avoid SPI cursor invalidation */
    typedef struct {
        int64 id;
        char broker_name[PGMQTTPUB_MAX_BROKER_NAME];
        char topic[PGMQTTPUB_MAX_TOPIC_LEN];
        bool success;
        int attempts;
    } MessageInfo;
    MessageInfo *messages = NULL;
    int num_messages = 0;

    /* Create a child memory context for this call to avoid accumulating memory
     * in the long-running worker loop. All allocations will be automatically
     * freed when we reset this context at the end. */
    MemoryContext drain_context = AllocSetContextCreate(
        CurrentMemoryContext,
        "drain_outbox",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldctx = MemoryContextSwitchTo(drain_context);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    snprintf(query, sizeof(query),
             "SELECT id, broker_name, topic, payload, qos, retain, attempts "
             "FROM mqtt_pub.outbox "
             "WHERE next_retry_at <= now() "
             "ORDER BY id "
             "LIMIT %d",
             pgmqttpub_outbox_batch_size);

    ret = SPI_execute(query, true, 0);
    processed = SPI_processed;

    if (ret != SPI_OK_SELECT || processed == 0)
    {
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();

        /* Clean up memory context before returning */
        MemoryContextSwitchTo(oldctx);
        MemoryContextReset(drain_context);
        return 0;
    }

    /* Allocate memory for message results.
     * Memory is bounded: SELECT query has LIMIT pgmqttpub_outbox_batch_size (default 500).
     * Max allocation: 500 messages × ~1069 bytes/message ≈ 534 KB.
     * Memory is contained within drain_context and automatically freed below. */
    messages = palloc(processed * sizeof(MessageInfo));
    num_messages = processed;

    /* Process each message and collect results */
    for (i = 0; i < processed; i++)
    {
        HeapTuple   tuple = SPI_tuptable->vals[i];
        TupleDesc   tupdesc = SPI_tuptable->tupdesc;
        bool        isnull;

        int64  id          = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
        char  *broker_name = SPI_getvalue(tuple, tupdesc, 2);
        char  *topic       = SPI_getvalue(tuple, tupdesc, 3);
        bytea *payload_b   = DatumGetByteaPP(SPI_getbinval(tuple, tupdesc, 4, &isnull));
        int    qos         = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 5, &isnull));
        bool   retain      = DatumGetBool(SPI_getbinval(tuple, tupdesc, 6, &isnull));
        int    attempts    = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 7, &isnull));

        void  *payload_data = VARDATA_ANY(payload_b);
        int    payload_len  = VARSIZE_ANY_EXHDR(payload_b);

        /* Try to publish message */
        bool ok = publish_to_broker(broker_name, topic, payload_data,
                                     payload_len, qos, retain);

        /* Store result for later processing */
        messages[i].id = id;
        strlcpy(messages[i].broker_name, broker_name, PGMQTTPUB_MAX_BROKER_NAME);
        strlcpy(messages[i].topic, topic, PGMQTTPUB_MAX_TOPIC_LEN);
        messages[i].success = ok;
        messages[i].attempts = attempts;

        if (ok)
            published++;
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* NOW do the DML operations after we've finished with the SELECT cursor */
    for (i = 0; i < num_messages; i++)
    {
        int ret;

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        if (messages[i].success)
        {
            /* Delete successful messages using parameterized query */
            Oid argtypes[1] = { INT8OID };
            Datum values[1];
            char nulls[1] = { ' ' };

            values[0] = Int64GetDatum(messages[i].id);

            ret = SPI_execute_with_args(
                "DELETE FROM mqtt_pub.outbox WHERE id = $1",
                1, argtypes, values, nulls, false, 0);

            if (ret != SPI_OK_DELETE)
                elog(WARNING, "pg_mqtt_pub: DELETE failed (rc=%d) for message id=%ld",
                     ret, messages[i].id);
            else
                pg_atomic_fetch_sub_u64(&pgmqttpub_shared->outbox_pending, 1);
        }
        else
        {
            int new_attempts = messages[i].attempts + 1;

            if (new_attempts >= pgmqttpub_poison_max_attempts)
            {
                /* Dead-letter the message using parameterized INSERT */
                BrokerHandle *h = find_handle_for_broker(messages[i].broker_name);
                const char *error_msg = h ? pgmqttpub_shared->broker_states[h->broker_idx].last_error
                                         : "broker not found";

                Oid argtypes[4] = { INT8OID, INT4OID, TEXTOID, INT8OID };
                Datum values[4];
                char nulls[4] = { ' ', ' ', ' ', ' ' };

                values[0] = Int64GetDatum(messages[i].id);
                values[1] = Int32GetDatum(new_attempts);
                values[2] = CStringGetTextDatum(error_msg);
                values[3] = Int64GetDatum(messages[i].id);

                ret = SPI_execute_with_args(
                    "INSERT INTO mqtt_pub.dead_letters "
                    "(original_id, broker_name, topic, payload, qos, retain, "
                    " attempts, first_failed_at, last_error) "
                    "SELECT id, broker_name, topic, payload, qos, retain, "
                    "       $2, created_at, $3 "
                    "FROM mqtt_pub.outbox WHERE id = $4",
                    4, argtypes, values, nulls, false, 0);

                if (ret != SPI_OK_INSERT)
                {
                    elog(WARNING, "pg_mqtt_pub: INSERT to dead_letters failed (rc=%d) for message id=%ld",
                         ret, messages[i].id);
                }
                else
                {
                    /* Now delete from outbox */
                    Oid del_argtypes[1] = { INT8OID };
                    Datum del_values[1];
                    char del_nulls[1] = { ' ' };

                    del_values[0] = Int64GetDatum(messages[i].id);

                    ret = SPI_execute_with_args(
                        "DELETE FROM mqtt_pub.outbox WHERE id = $1",
                        1, del_argtypes, del_values, del_nulls, false, 0);

                    if (ret != SPI_OK_DELETE)
                        elog(WARNING, "pg_mqtt_pub: DELETE after dead-letter failed (rc=%d) for message id=%ld",
                             ret, messages[i].id);
                    else
                    {
                        pg_atomic_fetch_sub_u64(&pgmqttpub_shared->outbox_pending, 1);
                        pg_atomic_fetch_add_u64(&pgmqttpub_shared->total_dead_lettered, 1);

                        if (h)
                        {
                            PgMqttPubBrokerState *bs = &pgmqttpub_shared->broker_states[h->broker_idx];
                            bs->messages_dead_lettered++;
                        }
                    }
                }

                elog(WARNING, "pg_mqtt_pub: dead-lettered message id=%ld "
                     "topic='%s' after %d attempts",
                     (long)messages[i].id, messages[i].topic, new_attempts);
            }
            else
            {
                /* Retry with exponential backoff using parameterized query */
                int backoff_ms;
                char backoff_interval[64];
                Oid argtypes[3] = { INT4OID, TEXTOID, INT8OID };
                Datum values[3];
                char nulls[3] = { ' ', ' ', ' ' };

                backoff_ms = PGMQTTPUB_POISON_BACKOFF_BASE_MS * (1 << (new_attempts - 1));
                if (backoff_ms > PGMQTTPUB_POISON_BACKOFF_CAP_MS)
                    backoff_ms = PGMQTTPUB_POISON_BACKOFF_CAP_MS;

                snprintf(backoff_interval, sizeof(backoff_interval),
                         "%d milliseconds", backoff_ms);

                values[0] = Int32GetDatum(new_attempts);
                values[1] = CStringGetTextDatum(backoff_interval);
                values[2] = Int64GetDatum(messages[i].id);

                ret = SPI_execute_with_args(
                    "UPDATE mqtt_pub.outbox "
                    "SET attempts = $1, "
                    "    next_retry_at = now() + interval $2 "
                    "WHERE id = $3",
                    3, argtypes, values, nulls, false, 0);

                if (ret != SPI_OK_UPDATE)
                    elog(WARNING, "pg_mqtt_pub: UPDATE (retry) failed (rc=%d) for message id=%ld",
                         ret, messages[i].id);
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    /* Switch back to original context and clean up drain context.
     * This automatically frees ALL memory allocated within drain_context,
     * including the messages array and any other allocations. */
    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(drain_context);

    return published;
}

/* ───────── Check if ALL Brokers Are Connected ───────── */

static bool
all_brokers_connected(void)
{
    int i;
    for (i = 0; i < num_handles; i++)
    {
        if (!handles[i].connected)
            return false;
    }
    return (num_handles > 0);
}

/* ───────── Check if Outbox is Empty ───────── */

static bool
outbox_is_empty(void)
{
    return pg_atomic_read_u64(&pgmqttpub_shared->outbox_pending) == 0;
}

/* ───────── Dead Letter Pruning ───────── */

static void
prune_dead_letters(void)
{
    int  ret;
    Oid  argtypes[1] = { INT4OID };
    Datum values[1];
    char nulls[1] = { ' ' };

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    values[0] = Int32GetDatum(pgmqttpub_dead_letter_retain_days);

    ret = SPI_execute_with_args(
        "DELETE FROM mqtt_pub.dead_letters "
        "WHERE dead_lettered_at < now() - interval '1 day' * $1",
        1, argtypes, values, nulls, false, 0);

    if (ret != SPI_OK_DELETE)
    {
        elog(WARNING, "pg_mqtt_pub: prune dead letters failed (rc=%d)", ret);
    }
    else if (SPI_processed > 0)
    {
        elog(LOG, "pg_mqtt_pub: pruned %lu expired dead letters",
             (unsigned long)SPI_processed);
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/* ═══════════════════════════════════════════
 *  Background Worker Main Loop
 * ═══════════════════════════════════════════ */

void
pgmqttpub_worker_main(Datum main_arg)
{
    PgMqttPubMessage msg;
    TimestampTz      last_prune = 0;
    int              i;

    /* Signal handlers */
    pqsignal(SIGHUP,  pgmqttpub_sighup_handler);
    pqsignal(SIGTERM, pgmqttpub_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    /* Connect to the configured outbox database for SPI access
     * If the database doesn't exist yet (e.g., during initial startup),
     * we'll retry on the next iteration. Don't fail fatally.
     */
    PG_TRY();
    {
        BackgroundWorkerInitializeConnection(pgmqttpub_outbox_database, NULL, 0);
    }
    PG_CATCH();
    {
        /* Database doesn't exist yet or other connection error - we'll retry later */
        FlushErrorState();
        /* Exit gracefully so we can retry */
        return;
    }
    PG_END_TRY();

    /* Attach shared memory */
    if (!pgmqttpub_shared)
    {
        bool found;
        pgmqttpub_shared = ShmemInitStruct("pg_mqtt_pub",
                                            sizeof(PgMqttPubSharedState),
                                            &found);
        if (!found)
        {
            elog(ERROR, "pg_mqtt_pub: shared memory not found");
            proc_exit(1);
        }
    }
    
    pgmqttpub_shared->worker_running = true;
    pgmqttpub_shared->worker_pid = MyProcPid;

    elog(LOG, "pg_mqtt_pub: background worker started (pid=%d)", MyProcPid);

    /* Initialize libmosquitto */
    mosquitto_lib_init();

    /* Check if outbox has pending rows from before crash/restart */
    PG_TRY();
    {
        int ret;
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        /* Check if extension exists first */
        ret = SPI_execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_mqtt_pub'", true, 0);
        if (ret != SPI_OK_SELECT || SPI_processed == 0)
        {
            /* Extension not installed - skip outbox check */
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            goto skip_outbox_check;
        }

        ret = SPI_execute("SELECT count(*) FROM mqtt_pub.outbox", true, 0);
        if (ret == SPI_OK_SELECT && SPI_processed > 0)
        {
            int64 pending = DatumGetInt64(
                SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
                              &(bool){false}));
            pg_atomic_write_u64(&pgmqttpub_shared->outbox_pending, pending);

            if (pending > 0)
            {
                switch_to_cold_mode();
                elog(LOG, "pg_mqtt_pub: found %ld pending outbox messages from prior session",
                     (long)pending);
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        /* Extension not yet created in this database - silently continue */
        FlushErrorState();
        AbortCurrentTransaction();
        elog(DEBUG1, "pg_mqtt_pub: outbox table not found, extension may not be created yet");
    }
    PG_END_TRY();

skip_outbox_check:
    /* Set up broker connections */
    refresh_broker_connections();

    /* ── Main Loop ── */

    while (!got_sigterm)
    {
        int  i;
        int  rc;
        int  messages_processed = 0;
        bool any_disconnected = false;

        /* Handle config reload */
        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
            refresh_broker_connections();
        }

        /* Drive mosquitto event loops and attempt reconnects */
        for (i = 0; i < num_handles; i++)
        {
            if (!handles[i].connected)
            {
                any_disconnected = true;
                try_connect(&handles[i]);
            }

            rc = mosquitto_loop(handles[i].mosq, 0, 1);
            if (rc != MOSQ_ERR_SUCCESS && rc != MOSQ_ERR_CONN_LOST)
                mosquitto_reconnect_async(handles[i].mosq);
        }

        /* If any broker is down, ensure COLD mode */
        if (any_disconnected)
            switch_to_cold_mode();

        /*
         * PRIORITY 1: Drain outbox (cold path recovery)
         *
         * We drain the outbox BEFORE touching the ring buffer.
         * This preserves message ordering: outbox messages were
         * enqueued earlier (during outage), so they must be
         * delivered first.
         */
        if (!outbox_is_empty() && all_brokers_connected())
        {
            int drained = drain_outbox();
            messages_processed += drained;

            /* If outbox is now empty AND all brokers connected → go HOT */
            if (outbox_is_empty() && all_brokers_connected())
                switch_to_hot_mode();
        }

        /*
         * PRIORITY 2: Drain ring buffer (hot path)
         *
         * Only process ring buffer if we're in HOT mode or if
         * brokers are connected (handles stragglers from mode switch).
         */
        if (all_brokers_connected())
        {
            while (pgmqttpub_queue_pop(&msg))
            {
                if (!dispatch_ringbuf_message(&msg))
                {
                    /*
                     * Ring buffer message failed to publish.
                     * Spill it to the outbox for retry.
                     */
                    char topic[PGMQTTPUB_MAX_TOPIC_LEN + 1];
                    int  qos    = msg.flags & PGMQTTPUB_FLAG_QOS_MASK;
                    bool retain = (msg.flags & PGMQTTPUB_FLAG_RETAIN) != 0;

                    memcpy(topic, msg.data, msg.topic_len);
                    topic[msg.topic_len] = '\0';

                    SetCurrentStatementStartTimestamp();
                    StartTransactionCommand();
                    SPI_connect();
                    PushActiveSnapshot(GetTransactionSnapshot());

                    pgmqttpub_outbox_insert(msg.broker_name, topic,
                                             msg.data + msg.topic_len,
                                             msg.payload_len, qos, retain);

                    SPI_finish();
                    PopActiveSnapshot();
                    CommitTransactionCommand();

                    switch_to_cold_mode();
                    break; /* stop draining ring buffer, let outbox take over */
                }

                messages_processed++;

                if (messages_processed % 1000 == 0)
                {
                    CHECK_FOR_INTERRUPTS();
                    for (i = 0; i < num_handles; i++)
                        mosquitto_loop(handles[i].mosq, 0, 1);
                }
            }
        }

        /* Periodic dead letter pruning (once per hour) */
        {
            TimestampTz now = GetCurrentTimestamp();
            long secs;
            int microsecs;

            if (last_prune == 0)
                last_prune = now;

            TimestampDifference(last_prune, now, &secs, &microsecs);
            if (secs >= 3600)
            {
                prune_dead_letters();
                last_prune = now;
            }
        }

        /* Wait for work */
        if (messages_processed == 0)
        {
            int wait_ms = outbox_is_empty()
                        ? pgmqttpub_worker_poll_interval_ms
                        : PGMQTTPUB_OUTBOX_POLL_INTERVAL_MS;

            (void) WaitLatch(MyLatch,
                             WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                             wait_ms,
                             PG_WAIT_EXTENSION);
            ResetLatch(MyLatch);
        }
    }

    /* ── Cleanup ── */

    elog(LOG, "pg_mqtt_pub: background worker shutting down");

    for (i = 0; i < num_handles; i++)
    {
        if (handles[i].mosq)
        {
            mosquitto_disconnect(handles[i].mosq);
            mosquitto_destroy(handles[i].mosq);
        }
    }

    mosquitto_lib_cleanup();
    pgmqttpub_shared->worker_running = false;
    proc_exit(0);
}
