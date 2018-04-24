#!/usr/bin/env bash

export PGUSER=postgres

for pgsrv in pum_test_1 pum_test_2 pum_test_3; do
  printf "[${pgsrv}]\nhost=localhost\ndbname=${pgsrv}\nuser=postgres\n\n" >> ~/.pg_service.conf
  dropdb --if-exists ${pgsrv}
  createdb ${pgsrv}
done
