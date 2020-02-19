#!/usr/bin/env bash

export PGUSER=postgres

# determine the pg_service conf file location
if [ -f "$PGSYSCONFDIR/pg_service.conf" ]; then
    PGSERVICE_FILE=$PGSYSCONFDIR/pg_service.conf
else
    PGSERVICE_FILE="~/.pg_service.conf"
fi

for pgsrv in pum_test_1 pum_test_2 pum_test_3; do
  printf "[${pgsrv}]\nhost=localhost\ndbname=${pgsrv}\nuser=postgres\n\n" >> $PGSERVICE_FILE
  dropdb --if-exists ${pgsrv}
  createdb ${pgsrv}
done
