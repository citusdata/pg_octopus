/*-------------------------------------------------------------------------
 *
 * src/pg_octopus.c
 *
 * Implementation of the pg_octopus health checker.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* these are internal headers */
#include "health_check_metadata.h"

/* these are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "tcop/utility.h"

#define CONN_INFO_TEMPLATE "host=%s port=%u dbname=%s connect_timeout=%u"
#define MAX_CONN_INFO_SIZE 1024


typedef enum
{
	HEALTH_CHECK_INITIAL = 0,
	HEALTH_CHECK_CONNECTING = 1,
	HEALTH_CHECK_OK = 2,
	HEALTH_CHECK_RETRY = 3,
	HEALTH_CHECK_DEAD = 4
	
} HealthCheckState;

typedef struct HealthCheck
{
	NodeHealth *node;
	HealthCheckState state;
	PGconn *connection;
	bool readyToPoll;
	PostgresPollingStatusType pollingStatus;
	int numTries;
	struct timeval nextEventTime;

} HealthCheck;


void _PG_init(void);
static void PgOctopusWorkerMain(Datum arg);
static List * CreateHealthChecks(List *nodeHealthList);
static HealthCheck * CreateHealthCheck(NodeHealth *nodeHealth);
static void DoHealthChecks(List *healthCheckList);
static void ManageHealthCheck(HealthCheck *healthCheck, struct timeval currentTime);
static int WaitForEvent(List *healthCheckList);
static int CompareTimes(struct timeval *leftTime, struct timeval *rightTime);
static struct timeval SubtractTimes(struct timeval base, struct timeval subtract);
static struct timeval AddTimeMillis(struct timeval base, uint32 additionalMs);
static void LatchWait(struct timeval timeout);


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

			FD_SET(connectionFileDescriptor, &exceptionFileDescriptorSet);
			
			if (healthCheck->pollingStatus == PGRES_POLLING_READING)
			{
				FD_SET(connectionFileDescriptor, &readFileDescriptorSet);
			}
			else if (healthCheck->pollingStatus == PGRES_POLLING_WRITING)
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

		foreach(healthCheckCell, healthCheckList)
		{
			HealthCheck *healthCheck = (HealthCheck *) lfirst(healthCheckCell);
			PGconn *connection = healthCheck->connection;
			int connectionFileDescriptor = PQsocket(connection);
			bool readyToPoll = false;

			readyToPoll = FD_ISSET(connectionFileDescriptor, &readFileDescriptorSet) ||
						  FD_ISSET(connectionFileDescriptor, &writeFileDescriptorSet) ||
						  FD_ISSET(connectionFileDescriptor, &exceptionFileDescriptorSet);

			healthCheck->readyToPoll = readyToPoll;
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
				if (nodeHealth->healthState != NODE_HEALTH_BAD)
				{
					elog(LOG, "marking node %s:%d as unhealthy",
							  nodeHealth->nodeName,
							  nodeHealth->nodePort);

					SetNodeHealthState(healthCheck->node->nodeName,
									   healthCheck->node->nodePort,
									   NODE_HEALTH_BAD);
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
				healthCheck->pollingStatus = PGRES_POLLING_FAILED;
				healthCheck->state = HEALTH_CHECK_RETRY;
			}
			else
			{
				struct timeval timeoutTime = {0, 0};

				timeoutTime = AddTimeMillis(currentTime, HealthCheckTimeout);

				healthCheck->nextEventTime = timeoutTime;
				healthCheck->connection = connection;
				healthCheck->pollingStatus = PGRES_POLLING_WRITING;
				healthCheck->state = HEALTH_CHECK_CONNECTING;
			}

			healthCheck->numTries++;

			break;
		}

		case HEALTH_CHECK_CONNECTING:
		{
			PGconn *connection = healthCheck->connection;
			PostgresPollingStatusType pollingStatus = PGRES_POLLING_FAILED;

			if (CompareTimes(&healthCheck->nextEventTime, &currentTime) < 0)
			{
				struct timeval nextTryTime = {0, 0};

				PQfinish(connection);

				nextTryTime = AddTimeMillis(currentTime, HealthCheckRetryDelay);

				healthCheck->nextEventTime = nextTryTime;
				healthCheck->connection = NULL;
				healthCheck->pollingStatus = pollingStatus;
				healthCheck->state = HEALTH_CHECK_RETRY;
				break;
			}

			if (!healthCheck->readyToPoll)
			{
				break;
			}

			pollingStatus = PQconnectPoll(connection);
			if (pollingStatus == PGRES_POLLING_FAILED)
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

				if (nodeHealth->healthState != NODE_HEALTH_GOOD)
				{
					elog(LOG, "marking node %s:%d as healthy",
							  nodeHealth->nodeName,
							  nodeHealth->nodePort);

					SetNodeHealthState(healthCheck->node->nodeName,
									   healthCheck->node->nodePort,
									   NODE_HEALTH_GOOD);
				}

				healthCheck->connection = NULL;
				healthCheck->numTries = 0;
				healthCheck->state = HEALTH_CHECK_OK;
			}
			else
			{
				/* Health check is still connecting */
			}

			healthCheck->pollingStatus = pollingStatus;

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
 *
 * From:
 * http://www.gnu.org/software/libc/manual/html_node/Elapsed-Time.html
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

