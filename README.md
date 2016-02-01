# pg_octopus

pg_octopus is an extension for PostgreSQL that health checks a cluster of PostgreSQL nodes in the background.

## Installation

The easiest way to install pg_octopus is to build the sources from GitHub.

    git clone https://github.com/citusdata/pg_octopus.git
    cd pg_octopus
    PATH=/usr/local/pgsql/bin/:$PATH make
    sudo PATH=/usr/local/pgsql/bin/:$PATH make install

After installing the extension, run the following in psql:

    CREATE EXTENSION pg_octopus;
    
pg_octopus uses a background worker to perform health checks. To activate the background worker, add pg_octopus to the shared_preload_libraries in postgresql.conf and restart postgres.

    # in postgresql.conf
    shared_preload_libraries = 'pg_octopus'

## Usage

To create a health check for a server, simple add its address to the octopus.nodes table.

     postgres=# INSERT INTO octopus.nodes VALUES ('10.192.0.247', 5432);
     INSERT 0 1
     postgres=# SELECT * FROM octopus.nodes;
       node_name   | node_port | health_status 
     --------------+-----------+---------------
      10.192.0.246 |      5432 |             0
      10.192.0.247 |      5432 |             1
     (2 rows)

In the health_status column, 1 means health, 0 means unhealthy, -1 means unknown.

## Health checks

pg_octopus performs health-checks in rounds of 'health_check_period' by trying to connect to nodes using libpq. If it fails to connect within 'health_check_timeout', it tries again after 'health_check_retry_delay' for at most 'health_check_max_retries' times. The default configuration values are shown below.

    pg_octopus.health_check_period = 10000 # round duration (in ms)
    pg_octopus.health_check_timeout = 2000 # connection timeout (in ms)
    pg_octopus.health_check_max_retries = 2 # maximum number of re-tries
    pg_octopus.health_check_retry_delay = 1000 # time between consecutive re-tries (in ms)
    
Note that health_check_timeout + health_check_max_retries * (health_check_retry_delay + health_check_timeout) should be smaller than health_check_period.
