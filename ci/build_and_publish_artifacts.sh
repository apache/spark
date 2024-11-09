#!/bin/bash

if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME is not set. Please set SPARK_HOME to the root directory of your Spark distribution."
  exit 1
fi

export ARTIFACTORY_TOKEN=${DATA_PLATFORM_ARTIFACTORY_TOKEN#*=}

cd $SPARK_HOME
VERSION=$(build/mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n 1 | sed 's/-SNAPSHOT//')

echo "Building Spark jars for ${SPARK_VERSION}"

build/mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false
build/mvn -DskipTests clean package -Phive -Pkubernetes -Phadoop-cloud -Phive-thriftserver

GROUP_ID=org.apache.spark
SCALA_VERSION=2.12
MODULES=(
  "core,spark-core"
  "common/network-common,spark-network-common"
  "common/network-shuffle,spark-network-shuffle"
  "launcher,spark-launcher"
  "streaming,spark-streaming"
  "connector/kafka-0-10,spark-streaming-kafka-0-10"
  "connector/kafka-0-10-sql,spark-sql-kafka-0-10"
  "connector/kafka-0-10-token-provider,spark-token-provider-kafka-0-10"
  "connector/avro,spark-avro"
  "connector/protobuf,spark-protobuf"
  "connector/connect/common,spark-connect-common"
  "connector/connect/client/jvm,spark-connect-client-jvm"
  "connector/connect/server,spark-connect"
  "sql/core,spark-sql"
  "sql/hive,spark-hive"
  "sql/catalyst,spark-catalyst"
  "sql/hive-thriftserver,spark-hive-thriftserver"
  "sql/api,spark-sql-api"
  "resource-managers/kubernetes/core,spark-kubernetes"
  "hadoop-cloud,spark-hadoop-cloud"
  "tools,spark-tools"
)
for MODULE in "${MODULES[@]}"; do
  IFS=',' read -r DIR NAME <<< $MODULE
  echo $NAME
  $SPARK_HOME/ci/upload-jar.sh $GROUP_ID ${NAME}_${SCALA_VERSION} $VERSION ${DIR}/target/${NAME}_${SCALA_VERSION}-${VERSION}.jar
done