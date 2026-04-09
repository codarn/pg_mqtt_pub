/*
 * pg_mqtt_pub.h — PostgreSQL MQTT Publish Extension
 *
 * Uses a ring buffer in shared memory to transport messages from backends to
 * the background worker, which publishes them via libmosquitto.
 * libmosquitto handles message buffering and retries via QoS 1/2 protocol.
 *
 * Copyright (c) 2025, PostgreSQL License
 */

#ifndef PG_MQTT_PUB_H
#define PG_MQTT_PUB_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

/* ───────── PostgreSQL Internal Functions ───────── */

/* Declared to signal the background worker (not in public headers) */
extern PGPROC *BackendPidGetProc(pid_t pid);

/* ───────── Constants ───────── */

#define PGMQTTPUB_MAGIC               0x4D51  /* "MQ" */
#define PGMQTTPUB_MAX_TOPIC_LEN       1024
#define PGMQTTPUB_MAX_PAYLOAD_LEN     (16 * 1024)
#define PGMQTTPUB_MAX_BROKER_NAME     32
#define PGMQTTPUB_MAX_HOST_LEN        256
#define PGMQTTPUB_MAX_CRED_LEN        256
#define PGMQTTPUB_MAX_PATH_LEN        1024
#define PGMQTTPUB_DEFAULT_QUEUE_SIZE  1024
#define PGMQTTPUB_DRAIN_BATCH_SIZE    500

/* ───────── Broker Connection State ───────── */

typedef enum PgMqttPubConnState
{
    PGMQTTPUB_CONN_DISCONNECTED = 0,
    PGMQTTPUB_CONN_CONNECTING,
    PGMQTTPUB_CONN_CONNECTED,
    PGMQTTPUB_CONN_ERROR
} PgMqttPubConnState;

/* ───────── Broker Configuration (in shared memory) ───────── */

typedef struct PgMqttPubBrokerConfig
{
    char        name[PGMQTTPUB_MAX_BROKER_NAME];
    char        host[PGMQTTPUB_MAX_HOST_LEN];
    int         port;
    char        username[PGMQTTPUB_MAX_CRED_LEN];
    char        password[PGMQTTPUB_MAX_CRED_LEN];
    bool        use_tls;
    char        ca_cert_path[PGMQTTPUB_MAX_PATH_LEN];
    char        client_cert_path[PGMQTTPUB_MAX_PATH_LEN];
    char        client_key_path[PGMQTTPUB_MAX_PATH_LEN];
} PgMqttPubBrokerConfig;

/* ───────── Broker Runtime State (in shared memory) ───────── */

typedef struct PgMqttPubBrokerState
{
    PgMqttPubConnState  state;
    uint64              messages_sent;
    uint64              messages_failed;
    uint64              messages_dead_lettered;
    uint32              queue_depth;
    TimestampTz         connected_since;
    TimestampTz         disconnected_since;
    char                last_error[256];
} PgMqttPubBrokerState;

/* ───────── Ring Buffer Message Slot ───────── */

typedef struct PgMqttPubMessage
{
    uint16  magic;                              /* Sentinel: PGMQTTPUB_MAGIC */
    uint8   qos;                                /* 0-2, explicit QoS level */
    bool    retain;                             /* Explicit retain flag */
    char    topic[PGMQTTPUB_MAX_TOPIC_LEN];     /* Null-terminated topic */
    char    payload[PGMQTTPUB_MAX_PAYLOAD_LEN]; /* Null-terminated payload */
} PgMqttPubMessage;

/* ───────── Shared Memory Ring Buffer ───────── */

typedef struct PgMqttPubQueue
{
    LWLock     *lock;
    uint32      head;
    uint32      tail;
    uint32      capacity;
} PgMqttPubQueue;

/* ───────── Top-Level Shared Memory State ───────── */

typedef struct PgMqttPubSharedState
{
    LWLock                     *config_lock;
    PgMqttPubBrokerConfig       broker_config;          /* Single broker configuration */
    PgMqttPubBrokerState        broker_state;           /* Single broker runtime state */
    PgMqttPubQueue              queue;                  /* Shared ring buffer for all messages */
    char                        init_database[64];      /* database where extension is installed */
    pid_t                       worker_pid;             /* Background worker PID for signaling */

    /* Flexible array member - ring buffer messages (must be last) */
    PgMqttPubMessage            messages[];
} PgMqttPubSharedState;

/* ───────── Global Shared State Pointer ───────── */

extern PgMqttPubSharedState *pgmqttpub_shared;

/* ───────── GUC Variables ───────── */

extern int   pgmqttpub_queue_size;
extern char *pgmqttpub_init_database;

/* Broker configuration parameters */
extern char *pgmqttpub_broker_host;
extern int   pgmqttpub_broker_port;
extern char *pgmqttpub_broker_username;
extern char *pgmqttpub_broker_password;
extern bool  pgmqttpub_broker_use_tls;
extern char *pgmqttpub_broker_ca_cert;
extern char *pgmqttpub_broker_client_cert;
extern char *pgmqttpub_broker_client_key;

/* ───────── Queue Operations (hot path) ───────── */

/* Push message to ring buffer */
bool pgmqttpub_queue_push(const char *topic, const char *payload,
                          int qos, bool retain);

/* Pop message from ring buffer (called by background worker) */
bool pgmqttpub_queue_pop(PgMqttPubMessage *msg);

/* ───────── Background Worker Entry Point ───────── */

PGDLLEXPORT void pgmqttpub_worker_main(Datum main_arg);

#endif /* PG_MQTT_PUB_H */
