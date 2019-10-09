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

run_custom_test() {
    testCustomCluster1 || errorLogs
    testCustomCluster2 || errorLogs
    testCustomCluster3 || errorLogs
    testCustomCluster4 || errorLogs
    testCustomCluster5 || errorLogs
}

run_tests() {
  testCreateCluster1 || errorLogs
  testScaleCluster || errorLogs
  sleep 45
  testNoPodRestartsOccurred "my-spark-cluster" || errorLogs
  testDeleteCluster || errorLogs
  sleep 5

  testCreateCluster2 || errorLogs
  testDownloadedData || errorLogs
  sleep 5

  testFullConfigCluster || errorLogs
  sleep 5

  run_custom_test || errorLogs

  sleep 10
  testApp || appErrorLogs
  testAppResult || appErrorLogs
  testDeleteApp || appErrorLogs

  sleep 5
  testPythonApp || appErrorLogs
  testPythonAppResult || appErrorLogs

  testMetricServer || errorLogs
  logs
}

main() {
  export total=20
  export testIndex=0
  tear_down
  setup_testing_framework
  os::test::junit::declare_suite_start "operator/tests"
  cluster_up
  testCreateOperator || { ${BIN} get events; ${BIN} get pods; exit 1; }
  export operator_pod=`${BIN} get pod -l app.kubernetes.io/name=spark-operator -o='jsonpath="{.items[0].metadata.name}"' | sed 's/"//g'`
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
