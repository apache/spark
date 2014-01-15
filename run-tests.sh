#!/bin/bash

FWDIR="$(cd `dirname $0`; pwd)"

$FWDIR/sparkR pkg/tests/run-all.R
