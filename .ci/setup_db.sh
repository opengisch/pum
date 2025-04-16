#!/usr/bin/env bash

export PGUSER=postgres

# determine the pg_service conf file location
if [ -z "$PGSYSCONFDIR" ]; then
    PGSERVICE_FILE="$HOME/.pg_service.conf"
else
    PGSERVICE_FILE="$PGSYSCONFDIR/pg_service.conf"
fi

pgsrv="pum_test"
echo "Adding service ${pgsrv} to $PGSERVICE_FILE"
printf "[${pgsrv}]\nhost=localhost\ndbname=${pgsrv}\nuser=postgres\npassword=postgres\n\n" >> "$PGSERVICE_FILE"

psql -c "DROP DATABASE IF EXISTS ${pgsrv};" "service=${pgsrv} dbname=postgres"
psql -c "CREATE DATABASE  ${pgsrv};" "service=${pgsrv} dbname=postgres"
