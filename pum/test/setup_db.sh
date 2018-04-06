#!/usr/bin/env bash


printf "[pum_test_1]\nhost=localhost\ndbname=pum_test_1\nuser=postgres\n\n[pum_test_2]\nhost=localhost\ndbname=pum_test_2\nuser=postgres\n" > ~/.pg_service.conf

export PGUSER=postgres

createdb pum_test_1
createdb pum_test_2
