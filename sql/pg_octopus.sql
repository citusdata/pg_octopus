-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_octopus" to load this file. \quit

CREATE SCHEMA octopus

	CREATE TABLE nodes (
		node_name text not null,
		node_port integer not null,
		health_status integer default -1,
		PRIMARY KEY(node_name, node_port)
	);
