/*-------------------------------------------------------------------------
 *
 * src/health_check_metadata.c
 *
 * Implementation of functions related to health check metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "health_check_metadata.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


/* human-readable names for addressing columns of health check queries */
#define TLIST_NUM_NODE_NAME 1
#define TLIST_NUM_NODE_PORT 2
#define TLIST_NUM_HEALTH_STATUS 3


static void StartSPITransaction(void);
static void EndSPITransaction(void);


/*
 * LoadNodeHealthList loads a list of nodes of which to check the health.
 */
List *
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
NodeHealth *
TupleToNodeHealth(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	NodeHealth *nodeHealth = NULL;
	bool isNull = false;

	Datum nodeNameDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										TLIST_NUM_NODE_NAME, &isNull);
	Datum nodePortDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										TLIST_NUM_NODE_PORT, &isNull);
	Datum healthStateDatum = SPI_getbinval(heapTuple, tupleDescriptor,
											TLIST_NUM_HEALTH_STATUS, &isNull);

	nodeHealth = palloc0(sizeof(NodeHealth));
	nodeHealth->nodeName = TextDatumGetCString(nodeNameDatum);
	nodeHealth->nodePort = DatumGetInt32(nodePortDatum);
	nodeHealth->healthState = DatumGetInt32(healthStateDatum);

	return nodeHealth;
}


/*
 * SetNodeHealthState updates the health state of a node in the metadata.
 */
void
SetNodeHealthState(char *nodeName, uint16 nodePort, int healthState)
{
	StringInfoData query;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	StartSPITransaction();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE octopus.nodes "
					 "SET health_status = %d "
					 "WHERE node_name = %s AND node_port = %d",
					 healthState,
					 quote_literal_cstr(nodeName),
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
