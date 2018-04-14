#!/usr/bin/env bash

dropdb pum_test_1 && dropdb pum_test_2 && createdb pum_test_1 && createdb pum_test_2
dropdb pum_test_3 && createdb pum_test_3

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../scripts/pum test-and-upgrade \
  -pp pum_test_1 -pt pum_test_2 -pc pum_test_3 \
  -t qgep_sys.pum_info \
  -d delta/ \
  -f /tmp/qwat_dump \
  -i constraints views \
  --exclude-schemas public
