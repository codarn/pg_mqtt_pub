/*
 * pg_mqtt_pub.c — PostgreSQL MQTT Publish Extension
 *
 * SQL-callable functions, shared memory initialization, hybrid message routing.
 *
 * Hybrid delivery model:
 *   HOT path  → shared memory ring buffer  (broker connected, sub-ms)
 *   COLD path → mqtt_pub.outbox table      (broker down, WAL-durable)
 *
 * The background worker (pg_mqtt_pub_worker.c) drains the outbox FIRST,
 * then the ring buffer, so ordering is preserved across mode switches.
 *
 * Copyright (c) 2025, PostgreSQL License
 */

#include "postgres.h"

#include "pg_mqtt_pub.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* ═══════════════════════════════════════════
 *  Global State
 * ═══════════════════════════════════════════ */

PgMqttPubSharedState *pgmqttpub_shared = NULL;

/* GUC variables */
char *pgmqttpub_broker_host        = "localhost";
int   pgmqttpub_broker_port        = 1883;
char *pgmqttpub_broker_username    = "";
char *pgmqttpub_broker_password    = "";
bool  pgmqttpub_broker_use_tls     = false;
char *pgmqttpub_broker_ca_cert     = "";
char *pgmqttpub_broker_client_cert = "";
char *pgmqttpub_broker_client_key  = "";
int   pgmqttpub_queue_size         = PGMQTTPUB_DEFAULT_QUEUE_SIZE;
int   pgmqttpub_max_connections    = PGMQTTPUB_MAX_BROKERS;
int   pgmqttpub_reconnect_interval_ms   = 5000;
int   pgmqttpub_publish_timeout_ms      = 100;
int   pgmqttpub_worker_poll_interval_ms = 10;
int   pgmqttpub_poison_max_attempts     = PGMQTTPUB_POISON_MAX_ATTEMPTS_DEFAULT;
int   pgmqttpub_outbox_batch_size       = PGMQTTPUB_OUTBOX_BATCH_SIZE_DEFAULT;
int   pgmqttpub_dead_letter_retain_days = PGMQTTPUB_DEAD_LETTER_RETAIN_DAYS_DEFAULT;

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
    size = add_size(size, mul_size(pgmqttpub_queue_size, PGMQTTPUB_SLOT_SIZE));
    return size;
}

static void
pgmqttpub_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(pgmqttpub_shmem_size());
    RequestNamedLWLockTranche("pg_mqtt_pub", 2);
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
        pgmqttpub_shared->queue.lock  = &locks[1].lock;
        pgmqttpub_shared->queue.capacity = pgmqttpub_queue_size;

        pg_atomic_init_u32(&pgmqttpub_shared->queue.head, 0);
        pg_atomic_init_u32(&pgmqttpub_shared->queue.tail, 0);

        /* Hybrid delivery: start in HOT mode */
        pg_atomic_init_u32(&pgmqttpub_shared->delivery_mode, PGMQTTPUB_MODE_HOT);
        pg_atomic_init_u64(&pgmqttpub_shared->outbox_pending, 0);
        pg_atomic_init_u64(&pgmqttpub_shared->total_dead_lettered, 0);

        /* Initialize default broker from GUC settings */
        strlcpy(pgmqttpub_shared->brokers[0].name,
                PGMQTTPUB_DEFAULT_BROKER_NAME, PGMQTTPUB_MAX_BROKER_NAME);
        strlcpy(pgmqttpub_shared->brokers[0].host,
                pgmqttpub_broker_host, PGMQTTPUB_MAX_HOST_LEN);
        pgmqttpub_shared->brokers[0].port = pgmqttpub_broker_port;
        strlcpy(pgmqttpub_shared->brokers[0].username,
                pgmqttpub_broker_username, PGMQTTPUB_MAX_CRED_LEN);
        strlcpy(pgmqttpub_shared->brokers[0].password,
                pgmqttpub_broker_password, PGMQTTPUB_MAX_CRED_LEN);
        pgmqttpub_shared->brokers[0].use_tls = pgmqttpub_broker_use_tls;
        strlcpy(pgmqttpub_shared->brokers[0].ca_cert_path,
                pgmqttpub_broker_ca_cert, PGMQTTPUB_MAX_PATH_LEN);
        pgmqttpub_shared->brokers[0].active = true;

        pgmqttpub_shared->worker_running = false;
    }

    LWLockRelease(AddinShmemInitLock);
}

/* ═══════════════════════════════════════════
 *  Ring Buffer Operations (Hot Path)
 * ═══════════════════════════════════════════ */

bool
pgmqttpub_queue_push(const char *broker_name, const char *topic,
                     const char *payload, int payload_len,
                     int qos, bool retain)
{
    PgMqttPubQueue   *q;
    PgMqttPubMessage *slot;
    uint32            head, tail, next;
    int               topic_len;
    int               total_data_len;
    char             *slot_base;

    if (!pgmqttpub_shared)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_mqtt_pub: shared memory not initialized"),
                 errhint("Add pg_mqtt_pub to shared_preload_libraries.")));

    q = &pgmqttpub_shared->queue;
    topic_len = strlen(topic);

    total_data_len = topic_len + payload_len;
    if (total_data_len > (int)sizeof(((PgMqttPubMessage *)0)->data))
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("pg_mqtt_pub: message too large (%d bytes, max %lu)",
                        total_data_len,
                        (unsigned long)sizeof(((PgMqttPubMessage *)0)->data))));

    if (topic_len > PGMQTTPUB_MAX_TOPIC_LEN)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("pg_mqtt_pub: topic too long (%d, max %d)",
                        topic_len, PGMQTTPUB_MAX_TOPIC_LEN)));

    for (;;)
    {
        head = pg_atomic_read_u32(&q->head);
        tail = pg_atomic_read_u32(&q->tail);
        next = (head + 1) % q->capacity;

        if (next == tail)
        {
            if (pgmqttpub_publish_timeout_ms == 0)
                return false;

            pg_usleep(1000);
            continue;
        }

        if (pg_atomic_compare_exchange_u32(&q->head, &head, next))
            break;
    }

    slot_base = ((char *)&q[1]) + ((Size)head * PGMQTTPUB_SLOT_SIZE);
    slot = (PgMqttPubMessage *)slot_base;

    slot->magic       = PGMQTTPUB_MAGIC;
    slot->flags       = (qos & PGMQTTPUB_FLAG_QOS_MASK)
                      | (retain ? PGMQTTPUB_FLAG_RETAIN : 0);
    slot->topic_len   = topic_len;
    slot->payload_len = payload_len;

    strlcpy(slot->broker_name,
            broker_name ? broker_name : PGMQTTPUB_DEFAULT_BROKER_NAME,
            PGMQTTPUB_MAX_BROKER_NAME);

    memcpy(slot->data, topic, topic_len);
    memcpy(slot->data + topic_len, payload, payload_len);

    return true;
}

bool
pgmqttpub_queue_pop(PgMqttPubMessage *msg)
{
    PgMqttPubQueue *q;
    uint32          head, tail, next;
    char           *slot_base;

    if (!pgmqttpub_shared)
        return false;

    q = &pgmqttpub_shared->queue;

    for (;;)
    {
        tail = pg_atomic_read_u32(&q->tail);
        head = pg_atomic_read_u32(&q->head);

        if (tail == head)
            return false;

        next = (tail + 1) % q->capacity;

        slot_base = ((char *)&q[1]) + ((Size)tail * PGMQTTPUB_SLOT_SIZE);
        memcpy(msg, slot_base, PGMQTTPUB_SLOT_SIZE);

        if (msg->magic != PGMQTTPUB_MAGIC)
        {
            pg_atomic_compare_exchange_u32(&q->tail, &tail, next);
            continue;
        }

        if (pg_atomic_compare_exchange_u32(&q->tail, &tail, next))
            return true;
    }
}

/* ═══════════════════════════════════════════
 *  Outbox Table Insert (Cold Path)
 *
 *  INSERT INTO mqtt_pub.outbox (broker_name, topic, payload, qos, retain)
 *  VALUES ($1, $2, $3, $4, $5);
 *
 *  Called from SQL backends when delivery_mode == COLD.
 *  Uses SPI so it participates in the caller's transaction —
 *  if the INSERT that fired the trigger rolls back, the outbox
 *  row rolls back too, giving us transactional consistency.
 * ═══════════════════════════════════════════ */

bool
pgmqttpub_outbox_insert(const char *broker_name, const char *topic,
                        const char *payload, int payload_len,
                        int qos, bool retain)
{
    int  ret;
    bool was_connected;

    ret = SPI_connect();
    was_connected = (ret == SPI_ERROR_CONNECT);  /* true if already connected */

    if (!was_connected && ret != SPI_OK_CONNECT)
    {
        elog(WARNING, "pg_mqtt_pub: SPI_connect failed for outbox insert");
        return false;
    }

    {
        Oid     argtypes[5] = { TEXTOID, TEXTOID, BYTEAOID, INT4OID, BOOLOID };
        Datum   values[5];
        char    nulls[5] = { ' ', ' ', ' ', ' ', ' ' };
        bytea  *payload_bytea;

        /* Build bytea from raw payload */
        payload_bytea = (bytea *)palloc(VARHDRSZ + payload_len);
        SET_VARSIZE(payload_bytea, VARHDRSZ + payload_len);
        memcpy(VARDATA(payload_bytea), payload, payload_len);

        values[0] = CStringGetTextDatum(broker_name ? broker_name
                                                    : PGMQTTPUB_DEFAULT_BROKER_NAME);
        values[1] = CStringGetTextDatum(topic);
        values[2] = PointerGetDatum(payload_bytea);
        values[3] = Int32GetDatum(qos);
        values[4] = BoolGetDatum(retain);

        ret = SPI_execute_with_args(
            "INSERT INTO mqtt_pub.outbox "
            "(broker_name, topic, payload, qos, retain) "
            "VALUES ($1, $2, $3, $4, $5)",
            5, argtypes, values, nulls, false, 0);
    }

    if (!was_connected)
        SPI_finish();

    if (ret != SPI_OK_INSERT)
    {
        elog(WARNING, "pg_mqtt_pub: outbox insert failed (SPI rc=%d)", ret);
        return false;
    }

    pg_atomic_fetch_add_u64(&pgmqttpub_shared->outbox_pending, 1);
    return true;
}

/* ═══════════════════════════════════════════
 *  Hybrid Router
 *
 *  Checks delivery_mode and routes to ring buffer or outbox.
 *  This is the single entry point used by all SQL publish functions.
 * ═══════════════════════════════════════════ */

bool
pgmqttpub_route_message(const char *broker_name, const char *topic,
                        const char *payload, int payload_len,
                        int qos, bool retain)
{
    uint32 mode;

    if (!pgmqttpub_shared)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_mqtt_pub: shared memory not initialized"),
                 errhint("Add pg_mqtt_pub to shared_preload_libraries.")));

    mode = pg_atomic_read_u32(&pgmqttpub_shared->delivery_mode);

    if (mode == PGMQTTPUB_MODE_HOT)
    {
        /*
         * Hot path: try ring buffer first.
         * If the ring buffer is full, fall through to outbox
         * so we never silently drop messages.
         */
        if (pgmqttpub_queue_push(broker_name, topic, payload, payload_len,
                                  qos, retain))
            return true;

        /* Ring buffer full — spill to outbox */
        elog(DEBUG1, "pg_mqtt_pub: ring buffer full, spilling to outbox");
    }

    /* Cold path (or spill): durable outbox insert */
    return pgmqttpub_outbox_insert(broker_name, topic, payload, payload_len,
                                    qos, retain);
}

/* ═══════════════════════════════════════════
 *  Broker Management Helpers
 * ═══════════════════════════════════════════ */

int
pgmqttpub_find_broker(const char *name)
{
    int i;
    for (i = 0; i < PGMQTTPUB_MAX_BROKERS; i++)
    {
        if (pgmqttpub_shared->brokers[i].active &&
            strcmp(pgmqttpub_shared->brokers[i].name, name) == 0)
            return i;
    }
    return -1;
}

int
pgmqttpub_add_broker(PgMqttPubBrokerConfig *config)
{
    int i;

    LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);

    for (i = 0; i < PGMQTTPUB_MAX_BROKERS; i++)
    {
        if (pgmqttpub_shared->brokers[i].active &&
            strcmp(pgmqttpub_shared->brokers[i].name, config->name) == 0)
        {
            LWLockRelease(pgmqttpub_shared->config_lock);
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                     errmsg("pg_mqtt_pub: broker '%s' already exists", config->name)));
        }
    }

    for (i = 0; i < PGMQTTPUB_MAX_BROKERS; i++)
    {
        if (!pgmqttpub_shared->brokers[i].active)
        {
            memcpy(&pgmqttpub_shared->brokers[i], config, sizeof(PgMqttPubBrokerConfig));
            pgmqttpub_shared->brokers[i].active = true;
            LWLockRelease(pgmqttpub_shared->config_lock);
            return i;
        }
    }

    LWLockRelease(pgmqttpub_shared->config_lock);
    ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("pg_mqtt_pub: max broker connections reached (%d)",
                    PGMQTTPUB_MAX_BROKERS)));
    return -1;
}

bool
pgmqttpub_remove_broker(const char *name)
{
    int i;

    if (strcmp(name, PGMQTTPUB_DEFAULT_BROKER_NAME) == 0)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_mqtt_pub: cannot remove the default broker")));

    LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);

    for (i = 0; i < PGMQTTPUB_MAX_BROKERS; i++)
    {
        if (pgmqttpub_shared->brokers[i].active &&
            strcmp(pgmqttpub_shared->brokers[i].name, name) == 0)
        {
            memset(&pgmqttpub_shared->brokers[i], 0, sizeof(PgMqttPubBrokerConfig));
            memset(&pgmqttpub_shared->broker_states[i], 0, sizeof(PgMqttPubBrokerState));
            LWLockRelease(pgmqttpub_shared->config_lock);
            return true;
        }
    }

    LWLockRelease(pgmqttpub_shared->config_lock);
    return false;
}

/* ═══════════════════════════════════════════
 *  SQL Function Declarations
 * ═══════════════════════════════════════════ */

PG_FUNCTION_INFO_V1(mqtt_publish);
PG_FUNCTION_INFO_V1(mqtt_publish_json);
PG_FUNCTION_INFO_V1(mqtt_publish_batch);
PG_FUNCTION_INFO_V1(mqtt_broker_add);
PG_FUNCTION_INFO_V1(mqtt_broker_remove);
PG_FUNCTION_INFO_V1(mqtt_status);

/* ═══════════════════════════════════════════
 *  mqtt_publish(topic, payload, qos, retain, broker)
 *  Routes through hybrid delivery model.
 * ═══════════════════════════════════════════ */

Datum
mqtt_publish(PG_FUNCTION_ARGS)
{
    text   *topic_text;
    text   *payload_text;
    int     qos;
    bool    retain;
    text   *broker_text;
    char   *topic, *payload, *broker_name;
    int     payload_len;
    bool    result;

    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("pg_mqtt_pub: topic cannot be NULL")));

    topic_text   = PG_GETARG_TEXT_PP(0);
    payload_text = PG_ARGISNULL(1) ? cstring_to_text("") : PG_GETARG_TEXT_PP(1);
    qos          = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    retain       = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    broker_text  = PG_ARGISNULL(4) ? cstring_to_text(PGMQTTPUB_DEFAULT_BROKER_NAME)
                                   : PG_GETARG_TEXT_PP(4);

    if (qos < 0 || qos > 2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("pg_mqtt_pub: qos must be 0, 1, or 2")));

    topic       = text_to_cstring(topic_text);
    payload     = text_to_cstring(payload_text);
    payload_len = VARSIZE_ANY_EXHDR(payload_text);
    broker_name = text_to_cstring(broker_text);

    /* Route through hybrid model */
    result = pgmqttpub_route_message(broker_name, topic, payload, payload_len,
                                      qos, retain);

    PG_RETURN_BOOL(result);
}

/* ═══════════════════════════════════════════
 *  mqtt_publish_json(topic, data, qos, retain, broker)
 * ═══════════════════════════════════════════ */

Datum
mqtt_publish_json(PG_FUNCTION_ARGS)
{
    text   *topic_text;
    Jsonb  *jb;
    int     qos;
    bool    retain;
    text   *broker_text;
    char   *topic, *payload, *broker_name;
    bool    result;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("pg_mqtt_pub: topic and data cannot be NULL")));

    topic_text  = PG_GETARG_TEXT_PP(0);
    jb          = PG_GETARG_JSONB_P(1);
    qos         = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    retain      = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    broker_text = PG_ARGISNULL(4) ? cstring_to_text(PGMQTTPUB_DEFAULT_BROKER_NAME)
                                  : PG_GETARG_TEXT_PP(4);

    topic       = text_to_cstring(topic_text);
    payload     = JsonbToCString(NULL, &jb->root, VARSIZE(jb));
    broker_name = text_to_cstring(broker_text);

    result = pgmqttpub_route_message(broker_name, topic, payload, strlen(payload),
                                      qos, retain);

    PG_RETURN_BOOL(result);
}

/* ═══════════════════════════════════════════
 *  mqtt_publish_batch(topic_prefix, payloads, qos, retain, broker)
 * ═══════════════════════════════════════════ */

Datum
mqtt_publish_batch(PG_FUNCTION_ARGS)
{
    text       *prefix_text;
    ArrayType  *payloads_arr;
    int         qos;
    bool        retain;
    text       *broker_text;
    char       *prefix, *broker_name;
    Datum      *elems;
    bool       *nulls;
    int         nelems;
    int         count = 0;
    int         i;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("pg_mqtt_pub: topic_prefix and payloads cannot be NULL")));

    prefix_text  = PG_GETARG_TEXT_PP(0);
    payloads_arr = PG_GETARG_ARRAYTYPE_P(1);
    qos          = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    retain       = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    broker_text  = PG_ARGISNULL(4) ? cstring_to_text(PGMQTTPUB_DEFAULT_BROKER_NAME)
                                   : PG_GETARG_TEXT_PP(4);

    prefix      = text_to_cstring(prefix_text);
    broker_name = text_to_cstring(broker_text);

    deconstruct_array(payloads_arr, TEXTOID, -1, false, TYPALIGN_INT,
                      &elems, &nulls, &nelems);

    for (i = 0; i < nelems; i++)
    {
        char *payload;
        char  topic[PGMQTTPUB_MAX_TOPIC_LEN];

        if (nulls[i])
            continue;

        payload = text_to_cstring(DatumGetTextPP(elems[i]));
        snprintf(topic, sizeof(topic), "%s/%d", prefix, i);

        if (pgmqttpub_route_message(broker_name, topic, payload, strlen(payload),
                                     qos, retain))
            count++;
    }

    PG_RETURN_INT32(count);
}

/* ═══════════════════════════════════════════
 *  mqtt_broker_add / mqtt_broker_remove
 * ═══════════════════════════════════════════ */

Datum
mqtt_broker_add(PG_FUNCTION_ARGS)
{
    PgMqttPubBrokerConfig config;

    memset(&config, 0, sizeof(config));

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("pg_mqtt_pub: name and host are required")));

    strlcpy(config.name, text_to_cstring(PG_GETARG_TEXT_PP(0)), PGMQTTPUB_MAX_BROKER_NAME);
    strlcpy(config.host, text_to_cstring(PG_GETARG_TEXT_PP(1)), PGMQTTPUB_MAX_HOST_LEN);
    config.port = PG_ARGISNULL(2) ? 1883 : PG_GETARG_INT32(2);

    if (!PG_ARGISNULL(3))
        strlcpy(config.username, text_to_cstring(PG_GETARG_TEXT_PP(3)), PGMQTTPUB_MAX_CRED_LEN);
    if (!PG_ARGISNULL(4))
        strlcpy(config.password, text_to_cstring(PG_GETARG_TEXT_PP(4)), PGMQTTPUB_MAX_CRED_LEN);

    config.use_tls = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5);

    if (!PG_ARGISNULL(6))
        strlcpy(config.ca_cert_path, text_to_cstring(PG_GETARG_TEXT_PP(6)), PGMQTTPUB_MAX_PATH_LEN);

    config.active = true;

    pgmqttpub_add_broker(&config);
    PG_RETURN_BOOL(true);
}

Datum
mqtt_broker_remove(PG_FUNCTION_ARGS)
{
    char *name;
    if (PG_ARGISNULL(0))
        PG_RETURN_BOOL(false);
    name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    PG_RETURN_BOOL(pgmqttpub_remove_broker(name));
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
        TupleDescInitEntry(tupdesc, 1,  "broker_name",       TEXTOID,  -1, 0);
        TupleDescInitEntry(tupdesc, 2,  "host",              TEXTOID,  -1, 0);
        TupleDescInitEntry(tupdesc, 3,  "port",              INT4OID,  -1, 0);
        TupleDescInitEntry(tupdesc, 4,  "connected",         BOOLOID,  -1, 0);
        TupleDescInitEntry(tupdesc, 5,  "delivery_mode",     TEXTOID,  -1, 0);
        TupleDescInitEntry(tupdesc, 6,  "messages_sent",     INT8OID,  -1, 0);
        TupleDescInitEntry(tupdesc, 7,  "messages_failed",   INT8OID,  -1, 0);
        TupleDescInitEntry(tupdesc, 8,  "dead_lettered",     INT8OID,  -1, 0);
        TupleDescInitEntry(tupdesc, 9,  "outbox_pending",    INT8OID,  -1, 0);
        TupleDescInitEntry(tupdesc, 10, "last_error",        TEXTOID,  -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = PGMQTTPUB_MAX_BROKERS;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx   = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;

    while (call_cntr < PGMQTTPUB_MAX_BROKERS)
    {
        if (pgmqttpub_shared->brokers[call_cntr].active)
        {
            Datum      values[10];
            bool       nulls[10];
            HeapTuple  tuple;
            PgMqttPubBrokerConfig *bc = &pgmqttpub_shared->brokers[call_cntr];
            PgMqttPubBrokerState  *bs = &pgmqttpub_shared->broker_states[call_cntr];
            uint32     mode = pg_atomic_read_u32(&pgmqttpub_shared->delivery_mode);

            memset(nulls, 0, sizeof(nulls));

            values[0] = CStringGetTextDatum(bc->name);
            values[1] = CStringGetTextDatum(bc->host);
            values[2] = Int32GetDatum(bc->port);
            values[3] = BoolGetDatum(bs->state == PGMQTTPUB_CONN_CONNECTED);
            values[4] = CStringGetTextDatum(mode == PGMQTTPUB_MODE_HOT ? "hot" : "cold");
            values[5] = Int64GetDatum(bs->messages_sent);
            values[6] = Int64GetDatum(bs->messages_failed);
            values[7] = Int64GetDatum(bs->messages_dead_lettered);
            values[8] = Int64GetDatum(pg_atomic_read_u64(&pgmqttpub_shared->outbox_pending));

            if (bs->last_error[0] != '\0')
                values[9] = CStringGetTextDatum(bs->last_error);
            else
                nulls[9] = true;

            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
        }

        funcctx->call_cntr = ++call_cntr;
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

    DefineCustomStringVariable("pg_mqtt_pub.broker_host",
                               "Default MQTT broker hostname",
                               NULL, &pgmqttpub_broker_host, "localhost",
                               PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.broker_port",
                            "Default MQTT broker port",
                            NULL, &pgmqttpub_broker_port, 1883, 1, 65535,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_username",
                               "Default MQTT broker username",
                               NULL, &pgmqttpub_broker_username, "",
                               PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_password",
                               "Default MQTT broker password",
                               NULL, &pgmqttpub_broker_password, "",
                               PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_mqtt_pub.broker_use_tls",
                             "Enable TLS for default broker",
                             NULL, &pgmqttpub_broker_use_tls, false,
                             PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_mqtt_pub.broker_ca_cert",
                               "Path to CA certificate for TLS",
                               NULL, &pgmqttpub_broker_ca_cert, "",
                               PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.queue_size",
                            "Ring buffer capacity (slots)",
                            NULL, &pgmqttpub_queue_size,
                            PGMQTTPUB_DEFAULT_QUEUE_SIZE, 64, 1048576,
                            PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.reconnect_interval_ms",
                            "Broker reconnection interval",
                            NULL, &pgmqttpub_reconnect_interval_ms, 5000, 100, 300000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.publish_timeout_ms",
                            "Max wait when ring buffer full (0 = spill to outbox immediately)",
                            NULL, &pgmqttpub_publish_timeout_ms, 100, 0, 30000,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.worker_poll_interval_ms",
                            "Background worker poll interval",
                            NULL, &pgmqttpub_worker_poll_interval_ms, 10, 1, 10000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.poison_max_attempts",
                            "Max delivery attempts before dead-lettering a message",
                            NULL, &pgmqttpub_poison_max_attempts,
                            PGMQTTPUB_POISON_MAX_ATTEMPTS_DEFAULT, 1, 100,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.outbox_batch_size",
                            "Max outbox rows drained per cycle",
                            NULL, &pgmqttpub_outbox_batch_size,
                            PGMQTTPUB_OUTBOX_BATCH_SIZE_DEFAULT, 1, 10000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_mqtt_pub.dead_letter_retain_days",
                            "Days to retain dead-lettered messages",
                            NULL, &pgmqttpub_dead_letter_retain_days,
                            PGMQTTPUB_DEAD_LETTER_RETAIN_DAYS_DEFAULT, 1, 365,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    /* ── Shared Memory Hooks ── */

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = pgmqttpub_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = pgmqttpub_shmem_startup;

    /* ── Register Background Worker ── */

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
