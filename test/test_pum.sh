#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

set -e
m=1
versions=("1.0.0" "1.0.1" "1.0.2" "1.1.0")
for ((i=1; i < ${#versions}-1; i++))
do
  p=$(($i-1))
  base=${versions[$p]}
  updated=${versions[$i]}
  echo "Testing upgrade from $base to $updated"
  ${DIR}/../.ci/setup_db.sh
  EXTRA_ARG=
  if [[ $updated =~ ^1\.1\.0$ ]]; then
    EXTRA_ARG=" -v int my_field_length 20"
  fi
  PGSERVICE=pum_test_1 psql --quiet -v ON_ERROR_STOP=on -f ${DIR}/data/create_northwind_$base.sql;
  PGSERVICE=pum_test_1 psql --quiet -v ON_ERROR_STOP=on -c "CREATE SCHEMA pum_sys";
  pum baseline -p pum_test_1 -t pum_sys.pum_info -d ${DIR}/data/delta/ -b $updated
  PGSERVICE=pum_test_2 psql --quiet -v ON_ERROR_STOP=on -f ${DIR}/data/create_northwind_$updated.sql;
  PGSERVICE=pum_test_2 psql --quiet -v ON_ERROR_STOP=on -c "CREATE SCHEMA pum_sys";
  pum baseline -p pum_test_2 -t pum_sys.pum_info -d ${DIR}/data/delta/ -b $updated

  yes | pum test-and-upgrade -pp pum_test_1 -pc pum_test_2 -pt pum_test_3 -t pum_sys.pum_info -d ${DIR}/data/delta/ -f /tmp/pum.dump -u $updated $EXTRA_ARG --exclude-field-pattern 'usr_%'



done
