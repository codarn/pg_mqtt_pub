/*
 * pg_mqtt_pub_worker.c — Single MQTT Broker Background Worker
 *
 * Architecture:
 *   - Single background worker manages one MQTT broker connection
 *   - Worker drains shared ring buffer and publishes messages
 *   - libmosquitto handles QoS, retries, buffering, and reconnection
 *   - MQTT broker is responsible for routing to multiple downstream destinations
 *
 * Copyright (c) 2025, PostgreSQL License
 */

#include "postgres.h"

#include "pg_mqtt_pub.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "lib/ilist.h"

#include <mosquitto.h>
#include <signal.h>
#include <pthread.h>

/* ───────── Worker State ───────── */

static volatile sig_atomic_t got_sigterm = false;

/* ───────── Mosquitto User Data ───────── */

typedef struct {
	char host[PGMQTTPUB_MAX_HOST_LEN];
	int  port;
} MosqUserData;

/* ───────── In-Flight Message Tracking ───────── */

typedef struct InflightEntry
{
	dlist_node			node;			/* Link in dlist (inflight/unclaimed/deadlettered) */
	int					mid;			/* Message ID from mosquitto */
	int					reason_code;	/* -1 = awaiting callback, 0x00 = success, >=0x80 = error */
	PgMqttPubMessage	message;		/* Full message (topic + payload) */
} InflightEntry;

static dlist_head inflight_list;       /* Entries awaiting MQTT ack */
static dlist_head unclaimed_list;      /* Entries ready for reuse */
static dlist_head deadlettered_list;   /* Entries waiting for DB insert (reason_code >= 0x80) */
static pthread_mutex_t lists_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Memory context for in-flight entry allocations (only allocates, doesn't free) */
static MemoryContext inflight_context = NULL;

/* ───────── Signal Handler ───────── */

static void
pgmqttpub_sigterm_handler(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/* ───────── Broker State Metrics ───────── */

static inline void
increment_messages_sent(void)
{
	LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
	pgmqttpub_shared->broker_state.messages_sent++;
	LWLockRelease(pgmqttpub_shared->config_lock);
}

static inline void
increment_messages_failed(void)
{
	LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
	pgmqttpub_shared->broker_state.messages_failed++;
	LWLockRelease(pgmqttpub_shared->config_lock);
}

static inline void
increment_messages_dead_lettered(void)
{
	LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
	pgmqttpub_shared->broker_state.messages_dead_lettered++;
	LWLockRelease(pgmqttpub_shared->config_lock);
}


/* ───────── Dead Letter Insert ───────── */

static void
dead_letter_insert(const PgMqttPubMessage *const msg,
				   const int mosq_error_code, const char *const error_msg)
{
	Oid argtypes[7] = {TEXTOID, TEXTOID, INT4OID, BOOLOID,
					   INT4OID, TEXTOID, TIMESTAMPTZOID};
	Datum values[7];
	char nulls[7] = {0};

	elog(LOG, "pg_mqtt_pub: inserting message into dead_letters (topic='%s', mosquitto_error_code=%d, error_message='%s')",
		 msg->topic, mosq_error_code, error_msg);

	/* Enter temporary memory context */
	MemoryContext tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
													  "dead_letter_insert",
													  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldcontext = MemoryContextSwitchTo(tmpcontext);
	int ret;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();

	PG_TRY();
	{
		ret = SPI_connect();
		if (ret != SPI_OK_CONNECT)
		{
			PG_RE_THROW();
		}

		PushActiveSnapshot(GetTransactionSnapshot());

		values[0] = CStringGetTextDatum(msg->topic);
		values[1] = CStringGetTextDatum(msg->payload);
		values[2] = Int32GetDatum(msg->qos);
		values[3] = BoolGetDatum(msg->retain);
		values[4] = Int32GetDatum(mosq_error_code);
		values[5] = CStringGetTextDatum(error_msg);
		values[6] = TimestampTzGetDatum(GetCurrentTimestamp());

		ret = SPI_execute_with_args(
			"INSERT INTO mqtt_pub.dead_letters "
			"(topic, payload, qos, retain, "
			" mosquitto_error_code, error_message, failed_at) "
			"VALUES ($1, $2, $3, $4, $5, $6, $7)",
			7, argtypes, values, nulls, false, 0);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(tmpcontext);
		AbortCurrentTransaction();
		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);

	/* Increment dead-lettered counter after successful insert */
	increment_messages_dead_lettered();
}

/* ───────── In-Flight Entry Creation ───────── */

static InflightEntry *
create_inflight_entry(const PgMqttPubMessage *const msg)
{
	InflightEntry *entry;

	if (msg->qos == 0)
	{
		/* QoS 0 does not require tracking */
		return NULL;
	}

	pthread_mutex_lock(&lists_mutex);

	/* Reuse from unclaimed pool if available */
	if (!dlist_is_empty(&unclaimed_list))
	{
		entry = dlist_container(InflightEntry, node,
								dlist_pop_head_node(&unclaimed_list));
		elog(DEBUG3, "pg_mqtt_pub: reused in-flight entry from pool");
	}
	else
	{
		/* Only allocate if pool is empty */
		pthread_mutex_unlock(&lists_mutex);
		entry = (InflightEntry *) MemoryContextAlloc(inflight_context,
													  sizeof(InflightEntry));
		pthread_mutex_lock(&lists_mutex);
		elog(DEBUG3, "pg_mqtt_pub: allocated new in-flight entry");
	}

	entry->mid = 0;  /* Will be set by mosquitto_publish */
	entry->reason_code = -1;  /* Pending callback */
	entry->message = *msg;

	/* Add to in-flight list */
	dlist_push_tail(&inflight_list, &entry->node);
	pthread_mutex_unlock(&lists_mutex);

	return entry;
}

/* ───────── Publish Message ───────── */

static bool
publish_message(struct mosquitto *const mosq, const PgMqttPubMessage *const msg)
{
	if (!mosq)
	{
		elog(ERROR, "pg_mqtt_pub: broker not initialized");
		increment_messages_failed();
		dead_letter_insert(msg, -1, "Broker not initialized");
		return false;
	}

	InflightEntry *entry = create_inflight_entry(msg);

	const int payload_len = strlen(msg->payload);

	int rc;
	if (entry)
		rc = mosquitto_publish(mosq, &entry->mid, msg->topic, payload_len,
							   msg->payload, msg->qos, msg->retain);
	else
		rc = mosquitto_publish(mosq, NULL, msg->topic, payload_len,
							   msg->payload, msg->qos, msg->retain);

	if (rc == MOSQ_ERR_SUCCESS)
	{
		increment_messages_sent();
		return true;
	}

	elog(DEBUG1, "pg_mqtt_pub: publish failed for topic '%s' (QoS %d): %s",
		msg->topic, msg->qos, mosquitto_strerror(rc));

	/* Publish failed - move entry to unclaimed pool for reuse */
	if (entry)
	{
		pthread_mutex_lock(&lists_mutex);
		dlist_delete(&entry->node);
		dlist_push_tail(&unclaimed_list, &entry->node);
		pthread_mutex_unlock(&lists_mutex);
	}

	/* QoS 0 "at most once" does not guarantee delivery, dead-lettering is not applicable */
	if (msg->qos > 0)
	{
		dead_letter_insert(msg, rc, mosquitto_strerror(rc));
	}

	increment_messages_failed();
	return false;
}

/* ───────── Broker Connection Callbacks ───────── */

static void
on_broker_connect(struct mosquitto *mosq, void *userdata, const int rc)
{
	const MosqUserData *userdata_ctx = (const MosqUserData *)userdata;

	if (rc == MOSQ_ERR_SUCCESS)
	{
		elog(LOG, "pg_mqtt_pub: broker connected successfully (%s:%d)",
			 userdata_ctx->host, userdata_ctx->port);

		/* Update connection timestamp in shared state */
		LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
		pgmqttpub_shared->broker_state.connected_since = GetCurrentTimestamp();
		LWLockRelease(pgmqttpub_shared->config_lock);
	}
	else
	{
		elog(WARNING, "pg_mqtt_pub: broker connection failed (%s:%d): %s",
			 userdata_ctx->host, userdata_ctx->port, mosquitto_strerror(rc));
	}
}

static void
on_broker_disconnect(struct mosquitto *mosq, void *userdata, const int rc)
{
	const MosqUserData *userdata_ctx = (const MosqUserData *)userdata;

	if (rc == MOSQ_ERR_SUCCESS)
	{
		elog(LOG, "pg_mqtt_pub: broker disconnected (%s:%d, client-initiated)",
			 userdata_ctx->host, userdata_ctx->port);
	}
	else
	{
		elog(WARNING, "pg_mqtt_pub: broker disconnected unexpectedly (%s:%d): %s",
			 userdata_ctx->host, userdata_ctx->port, mosquitto_strerror(rc));
	}

	/* Update disconnection timestamp in shared state (both clean and unexpected) */
	LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
	pgmqttpub_shared->broker_state.disconnected_since = GetCurrentTimestamp();
	LWLockRelease(pgmqttpub_shared->config_lock);
}

/* ───────── Publish Callback (MQTT v5) ───────── */

static void
on_publish_v5(struct mosquitto *mosq, void *userdata,
			  const int mid, const int reason_code, const mosquitto_property *props)
{
	elog(DEBUG1, "pg_mqtt_pub: publish callback received for mid=%d with reason_code=0x%02x",
		 mid, reason_code);

	pthread_mutex_lock(&lists_mutex);

	/* Find entry with matching message ID in in-flight list */
	dlist_mutable_iter iter;
	dlist_foreach_modify(iter, &inflight_list)
	{
		InflightEntry *entry = dlist_container(InflightEntry, node, iter.cur);

		if (entry->mid == mid)
		{
			if (reason_code == 0x00)
			{
				/* Success (MQTT v5 reason code 0x00) */
				elog(DEBUG1, "pg_mqtt_pub: message %d acknowledged", entry->mid);
				/* Remove from in-flight and move back to unclaimed pool */
				dlist_delete(&entry->node);
				dlist_push_tail(&unclaimed_list, &entry->node);
			}
			else if (entry->reason_code >= 0x80)
			{
				/* Failure (reason code >= 0x80 is error per MQTT 5.0 spec) */
				elog(DEBUG1, "pg_mqtt_pub: message %d rejected by broker (reason_code=0x%02x)",
					entry->mid, entry->reason_code);

				/* Move from in-flight to dead-letter queue for DB insert */
				dlist_delete(&entry->node);
				dlist_push_tail(&deadlettered_list, &entry->node);
			}
			else
			{
				/* 0x01-0x7F: Info codes (e.g., no matching subscribers) */
				elog(DEBUG1, "pg_mqtt_pub: message %d delivered with info code 0x%02x",
					entry->mid, entry->reason_code);
				/* Remove from in-flight and move back to unclaimed pool */
				dlist_delete(&entry->node);
				dlist_push_tail(&unclaimed_list, &entry->node);
			}
			break;
		}
	}

	/* If not found, callback may have fired after message was cleaned up (race).
	   This is benign—the message was already processed. */

	pthread_mutex_unlock(&lists_mutex);
}

/* ───────── Connect to Broker ───────── */

static struct mosquitto *
connect_broker(void)
{
	LWLockAcquire(pgmqttpub_shared->config_lock, LW_SHARED);
	const PgMqttPubBrokerConfig *const config = &pgmqttpub_shared->broker_config;

	/* Allocate and populate userdata while holding lock */
	MosqUserData *const userdata = palloc(sizeof(MosqUserData));
	strlcpy(userdata->host, config->host, sizeof(userdata->host));
	userdata->port = config->port;

	/* Create mosquitto instance with client ID */
	char client_id[128];
	snprintf(client_id, sizeof(client_id), "pg_mqtt_pub_%d", MyProcPid);

	struct mosquitto *mosq = mosquitto_new(client_id, false, (void *)userdata);
	if (!mosq)
	{
		LWLockRelease(pgmqttpub_shared->config_lock);
		elog(WARNING, "pg_mqtt_pub: mosquitto_new failed");
		return NULL;
	}

	/* Set connection callbacks */
	mosquitto_connect_callback_set(mosq, on_broker_connect);
	mosquitto_disconnect_callback_set(mosq, on_broker_disconnect);

	/* Set publish callback for QoS acknowledgments (MQTT v5) */
	mosquitto_publish_v5_callback_set(mosq, on_publish_v5);

	/* Set username/password if configured */
	if (config->username[0] != '\0')
		mosquitto_username_pw_set(mosq, config->username,
								  config->password[0] ? config->password : NULL);

	/* Configure TLS if needed */
	if (config->use_tls)
	{
		int rc = mosquitto_tls_set(mosq,
								   config->ca_cert_path[0] ? config->ca_cert_path : NULL,
								   NULL,
								   config->client_cert_path[0] ? config->client_cert_path : NULL,
								   config->client_key_path[0] ? config->client_key_path : NULL,
								   NULL);
		if (rc != MOSQ_ERR_SUCCESS)
		{
			elog(WARNING, "pg_mqtt_pub: TLS setup failed: %s", mosquitto_strerror(rc));
			mosquitto_destroy(mosq);
			LWLockRelease(pgmqttpub_shared->config_lock);
			return NULL;
		}
		mosquitto_tls_opts_set(mosq, 1, NULL, NULL);
	}

	/* Configure reconnection strategy */
	mosquitto_reconnect_delay_set(mosq, 1, 60, true);

	elog(LOG, "pg_mqtt_pub: connecting to %s:%d",
		config->host, config->port);

	/* Initiate async connection to broker */
	int rc = mosquitto_connect_async(mosq, config->host, config->port, 60);
	if (rc != MOSQ_ERR_SUCCESS)
	{
		elog(WARNING, "pg_mqtt_pub: mosquitto_connect_async failed: %s",
			 mosquitto_strerror(rc));
		mosquitto_destroy(mosq);
		LWLockRelease(pgmqttpub_shared->config_lock);
		return NULL;
	}

	LWLockRelease(pgmqttpub_shared->config_lock);
	return mosq;
}

/* ───────── Process Dead-Letter Queue ───────── */

static void
process_dead_letter_queue(void)
{
	elog(DEBUG1, "pg_mqtt_pub: processing dead-letter queue");

	pthread_mutex_lock(&lists_mutex);
	while (!dlist_is_empty(&deadlettered_list))
	{
		/* Pop entry from dead-letter list */
		InflightEntry *entry = dlist_container(InflightEntry, node,
											   dlist_pop_head_node(&deadlettered_list));

		/* Insert into database outside of the list lock */
		pthread_mutex_unlock(&lists_mutex);

		elog(DEBUG1, "pg_mqtt_pub: inserting dead-letter message (topic='%s', error_code=%d)",
			 entry->message.topic, entry->reason_code);
		dead_letter_insert(&entry->message, entry->reason_code,
						   mosquitto_reason_string(entry->reason_code));

		/* Reclaim the list lock */
		pthread_mutex_lock(&lists_mutex);

		/* Move entry to unclaimed pool for reuse */
		dlist_push_tail(&unclaimed_list, &entry->node);
	}
	pthread_mutex_unlock(&lists_mutex);
}

/* ───────── Background Worker Main ───────── */

void
pgmqttpub_worker_main(Datum main_arg)
{
	struct mosquitto *mosq = NULL;
	PgMqttPubMessage msg = {0};

	/* Setup signal handlers */
	pqsignal(SIGTERM, pgmqttpub_sigterm_handler);
	BackgroundWorkerUnblockSignals();

	/* Connect to database */
	PG_TRY();
	{
		BackgroundWorkerInitializeConnection(pgmqttpub_init_database, NULL, 0);
	}
	PG_CATCH();
	{
		FlushErrorState();
		proc_exit(1);
	}
	PG_END_TRY();

	/* Attach shared memory */
	if (!pgmqttpub_shared)
	{
		bool found;
		pgmqttpub_shared = ShmemInitStruct("pg_mqtt_pub",
										  0,  /* Size not used when not found */
										  &found);
		if (!found)
		{
			elog(ERROR, "pg_mqtt_pub: shared memory not found");
			proc_exit(1);
		}
	}

	elog(LOG, "pg_mqtt_pub: worker started (pid=%d)", MyProcPid);

	/* Initialize libmosquitto */
	mosquitto_lib_init();

	/* Initialize message tracking lists */
	dlist_init(&inflight_list);
	dlist_init(&unclaimed_list);
	dlist_init(&deadlettered_list);
	elog(DEBUG1, "pg_mqtt_pub: initialized in-flight, unclaimed, and dead-lettered lists");

	/* Create memory context for in-flight entries (batch freed after processing) */
	inflight_context = AllocSetContextCreate(TopMemoryContext,
											 "In-Flight Messages",
											 ALLOCSET_DEFAULT_SIZES);
	elog(DEBUG1, "pg_mqtt_pub: created in-flight message context");

	mosq = connect_broker();
	if (!mosq)
	{
		elog(FATAL, "pg_mqtt_pub: failed to initialize broker, exiting");
		proc_exit(0);
	}

	mosquitto_loop_start(mosq);

	/* Store our PID so backends can signal us (only after event loop is ready) */
	LWLockAcquire(pgmqttpub_shared->config_lock, LW_EXCLUSIVE);
	pgmqttpub_shared->worker_pid = MyProcPid;
	LWLockRelease(pgmqttpub_shared->config_lock);

	/* ── Main Loop ── */
	while (!got_sigterm)
	{
		elog(DEBUG1, "pg_mqtt_pub: waiting for latch");
		ResetLatch(MyLatch);

		/* Drain ring buffer in batches, yield to process dead-letter queue between batches */
		int drained;
		do
		{
			for (drained = 0; drained < PGMQTTPUB_DRAIN_BATCH_SIZE && pgmqttpub_queue_pop(&msg); drained++)
			{
				dead_letter_insert(&msg, 0, "Testing dead-letter insert");

				publish_message(mosq, &msg);
			}
			process_dead_letter_queue();
		} while (drained >= PGMQTTPUB_DRAIN_BATCH_SIZE);

		/* Wait for work if no messages processed */
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
	}

	/* ── Cleanup ── */

	elog(LOG, "pg_mqtt_pub: worker shutting down");

	if (mosq)
	{
		mosquitto_disconnect(mosq);
		/* Drain any remaining events */
		mosquitto_loop_stop(mosq, true);
		mosquitto_destroy(mosq);
	}

	mosquitto_lib_cleanup();

	/* Cleanup in-flight message tracking */
	process_dead_letter_queue();   /* Process any remaining dead-letter entries before shutdown */

	/* Delete in-flight context (frees all allocated entries) */
	if (inflight_context)
	{
		MemoryContextDelete(inflight_context);
		inflight_context = NULL;
	}

	pthread_mutex_destroy(&lists_mutex);

	proc_exit(0);
}
