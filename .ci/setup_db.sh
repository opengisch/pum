#!/usr/bin/env bash

export PGUSER=postgres

# determine the pg_service conf file location
if [ -z "$PGSYSCONFDIR" ]; then
    PGSERVICE_FILE="$HOME/.pg_service.conf"
else
    PGSERVICE_FILE="$PGSYSCONFDIR/pg_service.conf"
fi

for pgsrv in pum_test_1 pum_test_2 pum_test_3; do
  echo "Adding service ${pgsrv} to $PGSERVICE_FILE"
  printf "[${pgsrv}]\nhost=localhost\ndbname=${pgsrv}\nuser=postgres\npassword=postgres\n\n" >> "$PGSERVICE_FILE"
  dropdb --if-exists ${pgsrv}
  createdb ${pgsrv}
done
