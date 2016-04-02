/*-------------------------------------------------------------------------
 *
 * include/health_check_metadata.h
 *
 * Declarations for public functions and types related to health check
 * metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"


/*
 * NodeHealthState represents the last-known health state of a node after
 * the last round of health checks.
 */
typedef enum
{
	NODE_HEALTH_UNKNOWN = -1,
	NODE_HEALTH_BAD = 0,
	NODE_HEALTH_GOOD = 1

} NodeHealthState;

/*
 * NodeHealth represents a node that is to be health-checked and its last-known
 * health state.
 */
typedef struct NodeHealth
{
	char *nodeName;
	int nodePort;
	NodeHealthState healthState;

} NodeHealth;


/* GUC to change the health checks table */
extern char *HealthCheckNodesTable;


extern List * LoadNodeHealthList(void);
extern NodeHealth * TupleToNodeHealth(HeapTuple heapTuple,
												TupleDesc tupleDescriptor);
extern void SetNodeHealthState(char *nodeName, uint16 nodePort, int healthStatus);
