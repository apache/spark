#!/bin/bash

DIR="${DIR:-$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )}"
BIN=${BIN:-oc}
MANIFEST_SUFIX=${MANIFEST_SUFIX:-""}
if [ "$CRD" = "1" ]; then
  CR=""
  KIND="SparkCluster"
else
  CR="cm/"
  KIND="cm"
fi

source "${DIR}/.travis.test-common.sh"

run_tests() {
  testKillOperator || errorLogs
  testCreateCluster1 || errorLogs
  testKillOperator || errorLogs
  testScaleCluster || errorLogs
  testKillOperator || errorLogs
  testDeleteCluster || errorLogs
  testKillOperator || errorLogs

  sleep 10
  testApp || appErrorLogs
  testKillOperator || errorLogs
  testAppResult || appErrorLogs
  logs
}

main() {
  export total=11
  export testIndex=0
  tear_down
  setup_testing_framework
  os::test::junit::declare_suite_start "operator/tests-restarts"
  cluster_up
  testCreateOperator || { ${BIN} get events; ${BIN} get pods; exit 1; }
  if [ "$#" -gt 0 ]; then
    # run single test that is passed as arg
    $1
  else
    run_tests
  fi
  os::test::junit::declare_suite_end
  tear_down
}

main $@
