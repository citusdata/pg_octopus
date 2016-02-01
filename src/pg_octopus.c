#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

#define CONN_INFO_TEMPLATE "host=%s port=%u dbname=%s connect_timeout=%u"
#define MAX_CONN_INFO_SIZE 1024

#define TLIST_NUM_NODE_NAME 1
#define TLIST_NUM_NODE_PORT 2
#define TLIST_NUM_HEALTH_STATUS 3


typedef enum
{
	HEALTH_CHECK_INITIAL = 0,
	HEALTH_CHECK_CONNECTING = 1,
	HEALTH_CHECK_OK = 2,
	HEALTH_CHECK_RETRY = 3,
	HEALTH_CHECK_DEAD = 4
	
} HealthCheckState;

typedef struct NodeHealth
{
	char *nodeName;
	int nodePort;
	int healthStatus;

} NodeHealth;

typedef struct HealthCheck
{
	NodeHealth *node;
	HealthCheckState state;
	PGconn *connection;
	int numTries;
	struct timeval nextEventTime;

} HealthCheck;


void _PG_init(void);
static void PgOctopusWorkerMain(Datum arg);
static List * LoadNodeHealthList(void);
static NodeHealth * TupleToNodeHealth(HeapTuple heapTuple,
												TupleDesc tupleDescriptor);
static List * CreateHealthChecks(List *nodeHealthList);
static HealthCheck * CreateHealthCheck(NodeHealth *nodeHealth);
static void DoHealthChecks(List *healthCheckList);
static void ManageHealthCheck(HealthCheck *healthCheck, struct timeval currentTime);
static int WaitForEvent(List *healthCheckList);
static int CompareTimes(struct timeval *leftTime, struct timeval *rightTime);
static struct timeval SubtractTimes(struct timeval base, struct timeval subtract);
static struct timeval AddTimeMillis(struct timeval base, uint32 additionalMs);
static void LatchWait(struct timeval timeout);
static void SetHealth(char *nodeName, uint16 nodePort, int healthStatus);
static void StartSPITransaction(void);
static void EndSPITransaction(void);


PG_MODULE_MAGIC;


/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int HealthCheckPeriod = 10000;
static int HealthCheckTimeout = 2000;
static int HealthCheckMaxRetries = 2;
static int HealthCheckRetryDelay = 1000;


/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	DefineCustomIntVariable("pg_octopus.health_check_period",
							"Duration between each check (in milliseconds).",
							NULL, &HealthCheckPeriod, 10000, 1, INT_MAX, PGC_SIGHUP,
							0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_octopus.health_check_timeout",
							"Connect timeout (in milliseconds).",
							NULL, &HealthCheckTimeout, 2000, 1, INT_MAX, PGC_SIGHUP,
							0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_octopus.health_check_max_retries",
							"Maximum number of re-tries before marking a node as failed.",
							NULL, &HealthCheckMaxRetries, 2, 1, 100, PGC_SIGHUP,
							0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_octopus.health_check_retry_delay",
							"Delay between consecutive retries.",
							NULL, &HealthCheckRetryDelay, 1000, 1, INT_MAX, PGC_SIGHUP,
							0, NULL, NULL, NULL);


	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = PgOctopusWorkerMain;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;
	sprintf(worker.bgw_library_name, "pg_octopus");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_octopus_monitor");

	RegisterBackgroundWorker(&worker);
}


/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_octopus_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_octopus_sighup(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * PgOctopusWorkerMain is the main entry-point for the background worker
 * that performs health checks.
 */
static void
PgOctopusWorkerMain(Datum arg)
{
	MemoryContext healthCheckContext = NULL;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_octopus_sighup);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, pg_octopus_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	healthCheckContext = AllocSetContextCreate(CurrentMemoryContext,
											   "Health check context",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(healthCheckContext);

	elog(LOG, "pg_octopus monitor started");

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		struct timeval currentTime = {0, 0};
		struct timeval roundEndTime = {0, 0};
		struct timeval timeout = {0, 0};
		List *nodeHealthList = NIL;
		List *healthCheckList = NIL;

		gettimeofday(&currentTime, NULL);

		roundEndTime = AddTimeMillis(currentTime, HealthCheckPeriod);

		nodeHealthList = LoadNodeHealthList();
		healthCheckList = CreateHealthChecks(nodeHealthList);

		DoHealthChecks(healthCheckList);

		MemoryContextReset(healthCheckContext);

		gettimeofday(&currentTime, NULL);

		timeout = SubtractTimes(roundEndTime, currentTime);

		if (timeout.tv_sec >= 0 && timeout.tv_usec >= 0)
		{
			LatchWait(timeout);
		}

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	elog(LOG, "pg_octopus monitor exiting");

	proc_exit(0);
}


/*
 * LoadNodeHealthList loads a list of nodes of which to check the health.
 */
static List *
LoadNodeHealthList(void)
{
	List *nodeHealthList = NIL;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	StringInfoData query;

	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;

	StartSPITransaction();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT node_name, node_port, health_status "
					 "FROM octopus.nodes");

	pgstat_report_activity(STATE_RUNNING, query.data);

	spiStatus = SPI_execute(query.data, false, 0);
	Assert(spiStatus == SPI_OK_SELECT);

	oldContext = MemoryContextSwitchTo(upperContext);

	for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
	{
		HeapTuple heapTuple = SPI_tuptable->vals[rowNumber];
		NodeHealth *nodeHealth = TupleToNodeHealth(heapTuple,
																  SPI_tuptable->tupdesc);
		nodeHealthList = lappend(nodeHealthList, nodeHealth);
	}

	MemoryContextSwitchTo(oldContext);

	pgstat_report_activity(STATE_IDLE, NULL);

	EndSPITransaction();

	return nodeHealthList;
}


/*
 * TupleToNodeHealth constructs a node health description from a heap tuple obtained
 * via SPI.
 */
static NodeHealth *
TupleToNodeHealth(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	NodeHealth *nodeHealth = NULL;
	bool isNull = false;

	Datum nodeNameDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										TLIST_NUM_NODE_NAME, &isNull);
	Datum nodePortDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										TLIST_NUM_NODE_PORT, &isNull);
	Datum healthStatusDatum = SPI_getbinval(heapTuple, tupleDescriptor,
											TLIST_NUM_HEALTH_STATUS, &isNull);

	nodeHealth = palloc0(sizeof(NodeHealth));
	nodeHealth->nodeName = TextDatumGetCString(nodeNameDatum);
	nodeHealth->nodePort = DatumGetInt32(nodePortDatum);
	nodeHealth->healthStatus = DatumGetInt32(healthStatusDatum);

	return nodeHealth;
}


/*
 * CreateHealthChecks creates a list of health checks from a list of node health
 * descriptions.
 */
static List *
CreateHealthChecks(List *nodeHealthList)
{
	List *healthCheckList = NIL;
	ListCell *nodeHealthCell = NULL;

	foreach(nodeHealthCell, nodeHealthList)
	{
		NodeHealth *nodeHealth = (NodeHealth *) lfirst(nodeHealthCell);
		HealthCheck *healthCheck = CreateHealthCheck(nodeHealth);
		healthCheckList = lappend(healthCheckList, healthCheck);
	}

	return healthCheckList;
}


/*
 * CreateHealthCheck creates a health check from a health check description.
 */
static HealthCheck *
CreateHealthCheck(NodeHealth *nodeHealth)
{
	HealthCheck *healthCheck = NULL;
	struct timeval invalidTime = {0, 0};

	healthCheck = palloc0(sizeof(HealthCheck));
	healthCheck->node = nodeHealth;
	healthCheck->state = HEALTH_CHECK_INITIAL;
	healthCheck->connection = NULL;
	healthCheck->numTries = 0;
	healthCheck->nextEventTime = invalidTime;

	return healthCheck;
}


/*
 * DoHealthChecks performs the given health checks.
 */
static void
DoHealthChecks(List *healthCheckList)
{
	while (!got_sigterm)
	{
		int pendingCheckCount = 0;
		struct timeval currentTime = {0, 0};
		ListCell *healthCheckCell = NULL;

		gettimeofday(&currentTime, NULL);

		foreach(healthCheckCell, healthCheckList)
		{
			HealthCheck *healthCheck = (HealthCheck *) lfirst(healthCheckCell);

			ManageHealthCheck(healthCheck, currentTime);

			if (healthCheck->state != HEALTH_CHECK_OK &&
				healthCheck->state != HEALTH_CHECK_DEAD)
			{
				pendingCheckCount++;
			}
		}
		if (pendingCheckCount == 0)
		{
			break;
		}

		WaitForEvent(healthCheckList);
	}
}


/*
 * WaitForEvent sleeps until a time-based or I/O event occurs in any of the health
 * checks.
 */
static int
WaitForEvent(List *healthCheckList)
{
	ListCell *healthCheckCell = NULL;
	fd_set readFileDescriptorSet;
	fd_set writeFileDescriptorSet;
	fd_set exceptionFileDescriptorSet;
	struct timeval currentTime = {0, 0};
	struct timeval nextEventTime = {0, 0};
	struct timeval timeout = {0, 0};
	int maxFileDescriptor = 0;

	gettimeofday(&currentTime, NULL);

	FD_ZERO(&readFileDescriptorSet);
	FD_ZERO(&writeFileDescriptorSet);
	FD_ZERO(&exceptionFileDescriptorSet);

	foreach(healthCheckCell, healthCheckList)
	{
		HealthCheck *healthCheck = (HealthCheck *) lfirst(healthCheckCell);

		if (healthCheck->state == HEALTH_CHECK_CONNECTING ||
			healthCheck->state == HEALTH_CHECK_RETRY)
		{
			bool hasTimeout = healthCheck->nextEventTime.tv_sec != 0;

			if (hasTimeout &&
				(nextEventTime.tv_sec == 0 ||
				 CompareTimes(&healthCheck->nextEventTime, &nextEventTime) < 0))
			{
				nextEventTime = healthCheck->nextEventTime;
			}
		}

		if (healthCheck->state == HEALTH_CHECK_CONNECTING)
		{
			PGconn *connection = healthCheck->connection;
			int connectionFileDescriptor = PQsocket(connection);
			PostgresPollingStatusType pollingStatus = PQconnectPoll(connection);

			FD_SET(connectionFileDescriptor, &exceptionFileDescriptorSet);
			
			if (pollingStatus == PGRES_POLLING_READING)
			{
				FD_SET(connectionFileDescriptor, &readFileDescriptorSet);
			}
			else if (pollingStatus == PGRES_POLLING_WRITING)
			{
				FD_SET(connectionFileDescriptor, &writeFileDescriptorSet);
			}

			maxFileDescriptor = Max(maxFileDescriptor, connectionFileDescriptor);
		}
	}

	timeout = SubtractTimes(nextEventTime, currentTime);

	if (maxFileDescriptor >= 0)
	{
		int selectResult = 0;

		PG_SETMASK(&UnBlockSig);

		selectResult = select(maxFileDescriptor + 1, &readFileDescriptorSet,
							  &writeFileDescriptorSet, &exceptionFileDescriptorSet,
							  &timeout);

		PG_SETMASK(&BlockSig);

		if (selectResult < 0 && errno != EINTR && errno != EWOULDBLOCK)
		{
			return STATUS_ERROR;
		}
	}
	else
	{
		LatchWait(timeout);
	}

	return 0;
}


/*
 * LatchWait sleeps on the process latch until a timeout occurs.
 */
static void
LatchWait(struct timeval timeout)
{
	int waitResult = 0;
	long timeoutMs = timeout.tv_sec * 1000 + timeout.tv_usec / 1000;

	/*
	 * Background workers mustn't call usleep() or any direct equivalent:
	 * instead, they may wait on their process latch, which sleeps as
	 * necessary, but is awakened if postmaster dies.  That way the
	 * background process goes away immediately in an emergency.
	 */
	waitResult = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   timeoutMs);
	ResetLatch(MyLatch);

	/* emergency bailout if postmaster has died */
	if (waitResult & WL_POSTMASTER_DEATH)
	{
		proc_exit(1);
	}
}


/*
 * ManageHealthCheck proceeds the health check state machine.
 */
static void
ManageHealthCheck(HealthCheck *healthCheck, struct timeval currentTime)
{
	HealthCheckState checkState = healthCheck->state;
	NodeHealth *nodeHealth = healthCheck->node;
	
	switch (checkState)
	{
		case HEALTH_CHECK_RETRY:
		{
			if (healthCheck->numTries >= HealthCheckMaxRetries + 1)
			{
				if (nodeHealth->healthStatus == 1)
				{
					elog(LOG, "marking node %s:%d as unhealthy",
							  nodeHealth->nodeName,
							  nodeHealth->nodePort);

					SetHealth(healthCheck->node->nodeName,
							  healthCheck->node->nodePort,
							  0);
				}

				healthCheck->state = HEALTH_CHECK_DEAD;
				break;
			}

			if (CompareTimes(&healthCheck->nextEventTime, &currentTime) > 0)
			{
				/* Retry time lies in the future */
				break;
			}

			/* Fall through to re-connect */
		}

		case HEALTH_CHECK_INITIAL:
		{
			PGconn *connection = NULL;
			ConnStatusType connStatus = CONNECTION_BAD;
			char connInfoString[MAX_CONN_INFO_SIZE];
			
			snprintf(connInfoString, MAX_CONN_INFO_SIZE, CONN_INFO_TEMPLATE,
					 nodeHealth->nodeName, nodeHealth->nodePort, "postgres",
					 HealthCheckTimeout);

			connection = PQconnectStart(connInfoString);

			connStatus = PQstatus(connection);
			if (connStatus == CONNECTION_BAD)
			{
				struct timeval nextTryTime = {0, 0};

				PQfinish(connection);

				nextTryTime = AddTimeMillis(currentTime, HealthCheckRetryDelay);

				healthCheck->nextEventTime = nextTryTime;
				healthCheck->connection = NULL;
				healthCheck->state = HEALTH_CHECK_RETRY;
			}
			else
			{
				struct timeval timeoutTime = {0, 0};

				timeoutTime = AddTimeMillis(currentTime, HealthCheckTimeout);

				healthCheck->nextEventTime = timeoutTime;
				healthCheck->connection = connection;
				healthCheck->state = HEALTH_CHECK_CONNECTING;
			}

			healthCheck->numTries++;

			break;
		}

		case HEALTH_CHECK_CONNECTING:
		{
			PGconn *connection = healthCheck->connection;

			PostgresPollingStatusType pollingStatus = PQconnectPoll(connection);
			if (pollingStatus == PGRES_POLLING_FAILED ||
				CompareTimes(&healthCheck->nextEventTime, &currentTime) < 0)
			{
				struct timeval nextTryTime = {0, 0};

				PQfinish(connection);

				nextTryTime = AddTimeMillis(currentTime, HealthCheckRetryDelay);

				healthCheck->nextEventTime = nextTryTime;
				healthCheck->connection = NULL;
				healthCheck->state = HEALTH_CHECK_RETRY;
			}
			else if (pollingStatus == PGRES_POLLING_OK)
			{
				PQfinish(connection);

				if (nodeHealth->healthStatus == 0)
				{
					elog(LOG, "marking node %s:%d as healthy",
							  nodeHealth->nodeName,
							  nodeHealth->nodePort);

					SetHealth(healthCheck->node->nodeName,
							  healthCheck->node->nodePort,
							  1);
				}

				healthCheck->connection = NULL;
				healthCheck->numTries = 0;
				healthCheck->state = HEALTH_CHECK_OK;
			}
			else
			{
				/* Health check is still connecting */
			}

			break;
		}

		case HEALTH_CHECK_DEAD:
		case HEALTH_CHECK_OK:
		default:
		{
			/* Health check is done */
		}

	}
}


/*
 * CompareTime compares two timeval structs.
 *
 * If leftTime < rightTime, return -1
 * If leftTime > rightTime, return 1
 * else, return 0
 */
static int
CompareTimes(struct timeval *leftTime, struct timeval *rightTime)
{
	int compareResult = 0;
	
	if (leftTime->tv_sec < rightTime->tv_sec)
	{
		compareResult = -1;
	}
	else if (leftTime->tv_sec > rightTime->tv_sec)
	{
		compareResult = 1;
	}
	else if (leftTime->tv_usec < rightTime->tv_usec)
	{
		compareResult = -1;
	}
	else if (leftTime->tv_usec > rightTime->tv_usec)
	{
		compareResult = 1;
	}
	else
	{
		compareResult = 0;
	}

	return compareResult;
}

/*
 * SubtractTimes subtract the ‘struct timeval’ values y from x,
 * returning the result.
 */
static struct timeval
SubtractTimes(struct timeval x, struct timeval y)
{
	struct timeval result = {0, 0};

	/* Perform the carry for the later subtraction by updating y. */
	if (x.tv_usec < y.tv_usec)
	{
		int nsec = (y.tv_usec - x.tv_usec) / 1000000 + 1;
		y.tv_usec -= 1000000 * nsec;
		y.tv_sec += nsec;
	}

	if (x.tv_usec - y.tv_usec > 1000000)
	{
		int nsec = (x.tv_usec - y.tv_usec) / 1000000;
		y.tv_usec += 1000000 * nsec;
		y.tv_sec -= nsec;
	}

	/*
	 * Compute the time remaining to wait.
	 * tv_usec is certainly positive.
	 */
	result.tv_sec = x.tv_sec - y.tv_sec;
	result.tv_usec = x.tv_usec - y.tv_usec;

	return result;
}


/*
 * AddTimeMillis adds additionalMs milliseconds to a timeval.
 */
static struct timeval
AddTimeMillis(struct timeval base, uint32 additionalMs)
{
	struct timeval result = {0, 0};

	result.tv_sec = base.tv_sec + additionalMs / 1000;
	result.tv_usec = base.tv_usec + (additionalMs % 1000) * 1000;

	return result;
}


/*
 * SetHealth updates the health status of a node.
 */
static void
SetHealth(char *nodeName, uint16 nodePort, int healthStatus)
{
	StringInfoData query;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	StartSPITransaction();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE octopus.nodes "
					 "SET health_status = %d "
					 "WHERE node_name = '%s' AND node_port = %d",
					 healthStatus,
					 nodeName,
					 nodePort);

	pgstat_report_activity(STATE_RUNNING, query.data);

	spiStatus = SPI_execute(query.data, false, 0);
	Assert(spiStatus == SPI_OK_UPDATE);

	pgstat_report_activity(STATE_IDLE, NULL);

	EndSPITransaction();
}


/*
 * StartSPITransaction starts a transaction using SPI.
 */
static void
StartSPITransaction(void)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
}


/*
 * EndSPITransaction finishes a transaction that was started using SPI.
 */
static void
EndSPITransaction(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}
