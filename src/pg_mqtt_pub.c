/*
 * pg_mqtt_pub.c — PostgreSQL MQTT Publish Extension
 *
 * SQL-callable functions, shared memory initialization, and ring buffer transport.
 *
 * Simplified architecture (libmosquitto-buffered):
 *   - Ring buffer in shared memory transports messages from backends to worker
 *   - Background worker drains ring buffer and calls mosquitto_publish()
 *   - libmosquitto handles QoS 1/2 retries with persistent sessions
 *   - Any immediate publish failure → dead_letters table with error code
 *   - No outbox table or retry logic (libmosquitto handles durability)
 *
 * Copyright (c) 2025, PostgreSQL License
 */

#include "postgres.h"

#include "pg_mqtt_pub.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* ═══════════════════════════════════════════
 *  Global State
 * ═══════════════════════════════════════════ */

PgMqttPubSharedState *pgmqttpub_shared = NULL;

/* GUC variables */
int   pgmqttpub_queue_size         = PGMQTTPUB_DEFAULT_QUEUE_SIZE;
char *pgmqttpub_init_database      = NULL;
char *pgmqttpub_broker_host        = NULL;
int   pgmqttpub_broker_port        = 1883;
char *pgmqttpub_broker_username    = NULL;
char *pgmqttpub_broker_password    = NULL;
bool  pgmqttpub_broker_use_tls     = false;
char *pgmqttpub_broker_ca_cert     = NULL;
char *pgmqttpub_broker_client_cert = NULL;
char *pgmqttpub_broker_client_key  = NULL;

/* Shmem hooks */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ═══════════════════════════════════════════
 *  Shared Memory
 * ═══════════════════════════════════════════ */

static Size
pgmqttpub_shmem_size(void)
{
    Size size;

    size = sizeof(PgMqttPubSharedState);
    /* Flexible array member - ring buffer messages */
    size = add_size(size, mul_size(pgmqttpub_queue_size, sizeof(PgMqttPubMessage)));
    return size;
}

static void
pgmqttpub_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(pgmqttpub_shmem_size());
    /* 1 config lock + 1 queue lock */
    RequestNamedLWLockTranche("pg_mqtt_pub", 2);
}

static void
pgmqttpub_load_broker_config(void)
{
    PgMqttPubBrokerConfig config;

    /* Load broker configuration from GUC variables */
    memset(&config, 0, sizeof(PgMqttPubBrokerConfig));

    strlcpy(config.name, "default", PGMQTTPUB_MAX_BROKER_NAME);
    strlcpy(config.host, pgmqttpub_broker_host, PGMQTTPUB_MAX_HOST_LEN);
    config.port = pgmqttpub_broker_port;
    strlcpy(config.username, pgmqttpub_broker_username, PGMQTTPUB_MAX_CRED_LEN);
    strlcpy(config.password, pgmqttpub_broker_password, PGMQTTPUB_MAX_CRED_LEN);
    config.use_tls = pgmqttpub_broker_use_tls;
    strlcpy(config.ca_cert_path, pgmqttpub_broker_ca_cert, PGMQTTPUB_MAX_PATH_LEN);
    strlcpy(config.client_cert_path, pgmqttpub_broker_client_cert, PGMQTTPUB_MAX_PATH_LEN);
    strlcpy(config.client_key_path, pgmqttpub_broker_client_key, PGMQTTPUB_MAX_PATH_LEN);

    /* Store in shared memory */
    LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
    memcpy(&pgmqttpub_shared->broker_config, &config, sizeof(PgMqttPubBrokerConfig));
    LWLockRelease(pgmqttpub_shared->config_lock);

}

static void
pgmqttpub_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    pgmqttpub_shared = ShmemInitStruct("pg_mqtt_pub",
                                        pgmqttpub_shmem_size(),
                                        &found);

    if (!found)
    {
        LWLockPadded *locks = GetNamedLWLockTranche("pg_mqtt_pub");

        memset(pgmqttpub_shared, 0, pgmqttpub_shmem_size());

        pgmqttpub_shared->config_lock = &locks[0].lock;

        /* Initialize single shared ring buffer */
        pgmqttpub_shared->queue.lock = &locks[1].lock;
        pgmqttpub_shared->queue.capacity = pgmqttpub_queue_size;
        pgmqttpub_shared->queue.head = 0;
        pgmqttpub_shared->queue.tail = 0;

        /* Load broker configuration from GUC variables */
        pgmqttpub_load_broker_config();
    }

    LWLockRelease(AddinShmemInitLock);
}

/* ═══════════════════════════════════════════
 *  Ring Buffer Operations (Hot Path)
 * ═══════════════════════════════════════════ */

/* Helper to update queue depth in broker state.
 * Must be called while holding q->lock (queue lock).
 * Directly writes queue_depth without additional locking since
 * mqtt_status() reads under config_lock and uint32 is atomic.
 */
static inline void
update_queue_depth_under_queue_lock(PgMqttPubQueue *q)
{
    uint32 depth = (q->head >= q->tail) ?
        (q->head - q->tail) : (q->capacity - q->tail + q->head);

    pgmqttpub_shared->broker_state.queue_depth = depth;
}

bool
pgmqttpub_queue_push(const char *topic, const char *payload,
                     int qos, bool retain)
{
    PgMqttPubQueue   *q;
    PgMqttPubMessage msg = { 0 };
    uint32            head, next;
    PGPROC           *proc;

    if (!pgmqttpub_shared)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_mqtt_pub: shared memory not initialized"),
                 errhint("Add pg_mqtt_pub to shared_preload_libraries.")));
    }

    LWLockAcquire(pgmqttpub_shared->config_lock, LW_SHARED);
    if (!pgmqttpub_shared->worker_pid)
    {
        LWLockRelease(pgmqttpub_shared->config_lock);
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_mqtt_pub: background worker not running")));
    }
    proc = BackendPidGetProc(pgmqttpub_shared->worker_pid);
    LWLockRelease(pgmqttpub_shared->config_lock);

    /* Prepare message on stack before taking queue lock */
    msg.magic = PGMQTTPUB_MAGIC;
    msg.qos = qos;
    msg.retain = retain;

    /* Validate and copy topic */
    if (strlen(topic) >= PGMQTTPUB_MAX_TOPIC_LEN)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("pg_mqtt_pub: topic too long (%zu bytes, max %d)",
                        strlen(topic), PGMQTTPUB_MAX_TOPIC_LEN - 1)));
    }
    strlcpy(msg.topic, topic, PGMQTTPUB_MAX_TOPIC_LEN);

    /* Validate and copy payload */
    if (strlen(payload) >= PGMQTTPUB_MAX_PAYLOAD_LEN)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("pg_mqtt_pub: payload too large (%zu bytes, max %d)",
                        strlen(payload), PGMQTTPUB_MAX_PAYLOAD_LEN - 1)));
    }
    strlcpy(msg.payload, payload, PGMQTTPUB_MAX_PAYLOAD_LEN);

    /* Now acquire queue lock to place message */
    q = &pgmqttpub_shared->queue;
    LWLockAcquire(q->lock, LW_EXCLUSIVE);

    head = q->head;
    next = (head + 1) % q->capacity;

    if (next == q->tail)
    {
        LWLockRelease(q->lock);
        return false;
    }

    pgmqttpub_shared->messages[head] = msg;
    q->head = next;

    /* Update queue depth metric (while still holding queue lock) */
    update_queue_depth_under_queue_lock(q);

    LWLockRelease(q->lock);

    if (proc != NULL)
        SetLatch(&proc->procLatch);

    return true;
}

bool
pgmqttpub_queue_pop(PgMqttPubMessage *msg)
{
    PgMqttPubQueue *q;
    uint32          tail, next;

    if (!pgmqttpub_shared)
        return false;

    q = &pgmqttpub_shared->queue;

    LWLockAcquire(q->lock, LW_EXCLUSIVE);

    /* Check if queue is empty */
    if (q->tail == q->head)
    {
        LWLockRelease(q->lock);
        return false;
    }

    tail = q->tail;
    next = (tail + 1) % q->capacity;

    /* Copy message from ring buffer array */
    *msg = pgmqttpub_shared->messages[tail];
    q->tail = next;

    /* Update queue depth metric (while still holding queue lock) */
    update_queue_depth_under_queue_lock(q);

    LWLockRelease(q->lock);

    return true;
}

/* ═══════════════════════════════════════════
 *  SQL Function Declarations
 * ═══════════════════════════════════════════ */

PG_FUNCTION_INFO_V1(mqtt_publish);
PG_FUNCTION_INFO_V1(mqtt_status);

/* ═══════════════════════════════════════════
 *  mqtt_publish(broker, topic, payload, qos, retain)
 *  Routes through ring buffer to background worker.
 * ═══════════════════════════════════════════ */

Datum
mqtt_publish(PG_FUNCTION_ARGS)
{
    text   *topic_text;
    text   *payload_text;
    int     qos;
    bool    retain;
    char   *topic, *payload;
    bool    result;

    /* Topic is first required parameter */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("pg_mqtt_pub: topic cannot be NULL")));

    topic_text   = PG_GETARG_TEXT_PP(0);

    /* Payload with default */
    payload_text = PG_ARGISNULL(1) ? cstring_to_text("") : PG_GETARG_TEXT_PP(1);

    /* QoS with default */
    qos          = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);

    /* Retain with default */
    retain       = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);

    if (qos < 0 || qos > 2)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("pg_mqtt_pub: qos must be 0, 1, or 2")));
    }

    topic   = text_to_cstring(topic_text);
    payload = text_to_cstring(payload_text);

    /* Queue message to ring buffer */
    result = pgmqttpub_queue_push(topic, payload, qos, retain);

    if (!result)
    {
        if (qos == 0)
        {
            PG_RETURN_BOOL(false);
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("pg_mqtt_pub: ring buffer full, cannot queue QoS %d message for topic '%s'",
                            qos, topic),
                     errhint("Reduce batch size, increase pg_mqtt_pub.queue_size, or wait for worker to drain buffer")));
        }
    }
    PG_RETURN_BOOL(true);
}



/* ═══════════════════════════════════════════
 *  mqtt_status() — includes delivery mode + outbox depth
 * ═══════════════════════════════════════════ */

Datum
mqtt_status(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int              call_cntr;
    TupleDesc        tupdesc;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(10);
        TupleDescInitEntry(tupdesc, 1,  "host",              TEXTOID,        -1, 0);
        TupleDescInitEntry(tupdesc, 2,  "port",              INT4OID,        -1, 0);
        TupleDescInitEntry(tupdesc, 3,  "connected",         BOOLOID,        -1, 0);
        TupleDescInitEntry(tupdesc, 4,  "messages_sent",     INT8OID,        -1, 0);
        TupleDescInitEntry(tupdesc, 5,  "messages_failed",   INT8OID,        -1, 0);
        TupleDescInitEntry(tupdesc, 6,  "dead_lettered",     INT8OID,        -1, 0);
        TupleDescInitEntry(tupdesc, 7,  "queue_depth",       INT4OID,        -1, 0);
        TupleDescInitEntry(tupdesc, 8,  "connected_since",   TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, 9,  "disconnected_since",TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, 10, "worker_pid",        INT4OID,        -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = 1;  /* Single broker */

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx   = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;

    if (call_cntr < 1 && pgmqttpub_shared->worker_pid)
    {
        Datum      values[10];
        bool       nulls[10];
        HeapTuple  tuple;
        PgMqttPubBrokerConfig *bc;
        PgMqttPubBrokerState  *bs;
        bool       is_connected;

        memset(nulls, 0, sizeof(nulls));

        /* Read broker state under shared lock for consistency */
        LWLockAcquire(pgmqttpub_shared->config_lock, LW_SHARED);

        bc = &pgmqttpub_shared->broker_config;
        bs = &pgmqttpub_shared->broker_state;

        /* Compute connected: true if connected_since > disconnected_since */
        is_connected = (bs->connected_since != 0 &&
                       (bs->disconnected_since == 0 ||
                        bs->connected_since > bs->disconnected_since));

        values[0] = CStringGetTextDatum(bc->host);
        values[1] = Int32GetDatum(bc->port);
        values[2] = BoolGetDatum(is_connected);
        values[3] = Int64GetDatum(bs->messages_sent);
        values[4] = Int64GetDatum(bs->messages_failed);
        values[5] = Int64GetDatum(bs->messages_dead_lettered);
        values[6] = Int32GetDatum(bs->queue_depth);

        if (bs->connected_since != 0)
            values[7] = TimestampTzGetDatum(bs->connected_since);
        else
            nulls[7] = true;

        if (bs->disconnected_since != 0)
            values[8] = TimestampTzGetDatum(bs->disconnected_since);
        else
            nulls[8] = true;

        values[9] = Int32GetDatum(pgmqttpub_shared->worker_pid);

        LWLockRelease(pgmqttpub_shared->config_lock);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/* ═══════════════════════════════════════════
 *  Module Initialization (_PG_init)
 * ═══════════════════════════════════════════ */

void _PG_init(void);

void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        return;

    /* ── GUC Definitions ── */

    DefineCustomIntVariable("pg_mqtt_pub.queue_size",
                            "Ring buffer capacity (slots)",
                            NULL, &pgmqttpub_queue_size,
                            PGMQTTPUB_DEFAULT_QUEUE_SIZE, 64, 1048576,
                            PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.init_database",
                               "Database where pg_mqtt_pub extension is installed",
                               "Worker uses this to connect for SPI operations.",
                               &pgmqttpub_init_database,
                               "postgres",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    /* ── Broker Configuration Parameters ── */

    DefineCustomStringVariable("pg_mqtt_pub.broker_host",
                               "MQTT broker hostname or IP address",
                               "Host to connect to for MQTT broker",
                               &pgmqttpub_broker_host,
                               "localhost",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.broker_port",
                            "MQTT broker port",
                            "Port to connect to for MQTT broker (default 1883 for MQTT, 8883 for TLS)",
                            &pgmqttpub_broker_port, 1883, 1, 65535,
                            PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_username",
                               "MQTT broker username",
                               "Username for broker authentication (leave empty for anonymous)",
                               &pgmqttpub_broker_username,
                               "",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_password",
                               "MQTT broker password",
                               "Password for broker authentication (only used if username is set)",
                               &pgmqttpub_broker_password,
                               "",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_mqtt_pub.broker_use_tls",
                             "Use TLS for broker connection",
                             "Enable TLS/SSL encryption for MQTT broker connection",
                             &pgmqttpub_broker_use_tls, false,
                             PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_ca_cert",
                               "CA certificate path for TLS",
                               "Path to CA certificate file for TLS verification (PEM format)",
                               &pgmqttpub_broker_ca_cert,
                               "",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_client_cert",
                               "Client certificate path for TLS",
                               "Path to client certificate file for mutual TLS (PEM format)",
                               &pgmqttpub_broker_client_cert,
                               "",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_client_key",
                               "Client key path for TLS",
                               "Path to client private key file for mutual TLS (PEM format)",
                               &pgmqttpub_broker_client_key,
                               "",
                               PGC_POSTMASTER, 0, NULL, NULL, NULL);

    /* ── Shared Memory Hooks ── */

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = pgmqttpub_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = pgmqttpub_shmem_startup;

    /* ── Register Single Background Worker ── */

    memset(&worker, 0, sizeof(BackgroundWorker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "pg_mqtt_pub worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_mqtt_pub worker");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_mqtt_pub");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgmqttpub_worker_main");

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    worker.bgw_main_arg = Int32GetDatum(0);
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}
