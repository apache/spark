#!/bin/bash

FWDIR="$(cd `dirname $0`; pwd)"

$FWDIR/sparkR pkg/inst/tests/run-all.R
