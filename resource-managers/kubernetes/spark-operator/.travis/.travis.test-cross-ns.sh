#!/bin/bash

DIR="${DIR:-$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )}"
BIN=${BIN:-oc}
MANIFEST_SUFIX=${MANIFEST_SUFIX:-""}
if [ "$CRD" = "1" ]; then
  CM=""
  KIND="SparkCluster"
else
  CM="cm/"
  KIND="cm"
fi

source "${DIR}/.travis.test-common.sh"


testCreateClusterInNamespace() {
  info
  echo -e "\n\n namespace:\n"
  oc project
  [ "$CRD" = "0" ] && FOO="-cm" || FOO=""
  os::cmd::expect_success_and_text "${BIN} create -f $DIR/../examples/cluster$FOO.yaml" '"?my-spark-cluster"? created' && \
  os::cmd::try_until_text "${BIN} get pod -l radanalytics.io/deployment=my-spark-cluster-w -o yaml" 'ready: true' && \
  os::cmd::try_until_text "${BIN} get pod -l radanalytics.io/deployment=my-spark-cluster-m -o yaml" 'ready: true'
}

testDeleteClusterInNamespace() {
  info
  os::cmd::expect_success_and_text '${BIN} delete ${KIND} my-spark-cluster' '"my-spark-cluster" deleted'
  sleep 5
}


run_tests() {
  testEditOperator 'WATCH_NAMESPACE="*"' || errorLogs
  sleep 15 # wait long enough, because the full reconciliation is not supported for this use-case (*)

  os::cmd::expect_success_and_text '${BIN} new-project foo' 'Now using project' || errorLogs
  testCreateClusterInNamespace || errorLogs
  testDeleteClusterInNamespace || errorLogs
  os::cmd::expect_success_and_text '${BIN} new-project bar' 'Now using project' || errorLogs
  testCreateClusterInNamespace || errorLogs
  testDeleteClusterInNamespace || errorLogs

  sleep 5
  logs
}

main() {
  export total=6
  export testIndex=0
  tear_down
  setup_testing_framework
  os::test::junit::declare_suite_start "operator/tests-cross-ns"
  cluster_up
  # cluster role is required for the cross namespace watching
  oc login -u system:admin
  oc adm policy add-cluster-role-to-user cluster-admin -z spark-operator
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
