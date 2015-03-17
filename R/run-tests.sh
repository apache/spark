#!/bin/bash

FWDIR="$(cd `dirname $0`; pwd)"

FAILED=0
LOGFILE=unit-tests.log
rm -f $LOGFILE

$FWDIR/../bin/sparkR $FWDIR/pkg/tests/run-all.R >$LOGFILE 2>&1
FAILED=$((PIPESTATUS[0]||$FAILED))

if [[ $FAILED != 0 ]]; then
    cat $LOGFILE
    echo -en "\033[31m"  # Red
    echo "Had test failures; see logs."
    echo -en "\033[0m"  # No color
    exit -1
else
    echo -en "\033[32m"  # Green
    echo "Tests passed."
    echo -en "\033[0m"  # No color
fi
