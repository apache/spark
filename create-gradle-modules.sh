#!/bin/bash

# Script to create basic build.gradle.kts files for remaining modules

MODULES=(
    "common/sketch"
    "common/kvstore"
    "common/network-common"
    "common/network-shuffle"
    "common/unsafe"
    "common/variant"
    "common/network-yarn"
    "sql/api"
    "sql/catalyst"
    "sql/core"
    "sql/hive"
    "sql/pipelines"
    "sql/connect/server"
    "sql/connect/common"
    "sql/connect/client/jvm"
    "sql/connect/shims"
    "sql/hive-thriftserver"
    "graphx"
    "mllib"
    "mllib-local"
    "tools"
    "streaming"
    "assembly"
    "examples"
    "repl"
    "hadoop-cloud"
    "resource-managers/yarn"
    "resource-managers/kubernetes/core"
    "resource-managers/kubernetes/integration-tests"
    "connector/kafka-0-10-token-provider"
    "connector/kafka-0-10"
    "connector/kafka-0-10-assembly"
    "connector/kafka-0-10-sql"
    "connector/avro"
    "connector/protobuf"
    "connector/spark-ganglia-lgpl"
    "connector/kinesis-asl"
    "connector/kinesis-asl-assembly"
    "connector/docker-integration-tests"
    "connector/profiler"
)

for module in "${MODULES[@]}"; do
    if [ ! -f "$module/build.gradle.kts" ]; then
        module_name=$(basename "$module")
        echo "Creating build.gradle.kts for $module"

        cat > "$module/build.gradle.kts" << 'EOF'
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO: Add module-specific dependencies by analyzing corresponding pom.xml

dependencies {
    // Common dependencies will be inherited from spark-common plugin

    // Add module-specific dependencies here
    // implementation("group:artifact:version")
}
EOF
    fi
done

echo "Basic build.gradle.kts files created. Each module needs to be configured with its specific dependencies."