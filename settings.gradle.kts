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

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven("https://repository.apache.org/content/repositories/snapshots/")
        maven("https://repository.apache.org/content/groups/staging/")
    }
}

rootProject.name = "spark"

// Common modules
include("common:sketch")
include("common:kvstore")
include("common:network-common")
include("common:network-shuffle")
include("common:unsafe")
include("common:utils")
include("common:utils-java")
include("common:variant")
include("common:tags")
include("common:network-yarn")

// SQL modules
include("sql:connect:shims")
include("sql:api")
include("sql:catalyst")
include("sql:core")
include("sql:hive")
include("sql:pipelines")
include("sql:connect:server")
include("sql:connect:common")
include("sql:connect:client:jvm")
include("sql:hive-thriftserver")

// Core modules
include("core")
include("graphx")
include("mllib")
include("mllib-local")
include("tools")
include("streaming")

// Assembly and examples
include("assembly")
include("examples")
include("repl")
include("launcher")

// Connectors
include("connector:kafka-0-10-token-provider")
include("connector:kafka-0-10")
include("connector:kafka-0-10-assembly")
include("connector:kafka-0-10-sql")
include("connector:avro")
include("connector:protobuf")
include("connector:spark-ganglia-lgpl")
include("connector:kinesis-asl")
include("connector:kinesis-asl-assembly")
include("connector:docker-integration-tests")
include("connector:profiler")

// Resource managers
include("resource-managers:yarn")
include("resource-managers:kubernetes:core")
include("resource-managers:kubernetes:integration-tests")

// Hadoop cloud
include("hadoop-cloud")

// Set project directory mappings to match Maven module structure
project(":common:sketch").projectDir = file("common/sketch")
project(":common:kvstore").projectDir = file("common/kvstore")
project(":common:network-common").projectDir = file("common/network-common")
project(":common:network-shuffle").projectDir = file("common/network-shuffle")
project(":common:unsafe").projectDir = file("common/unsafe")
project(":common:utils").projectDir = file("common/utils")
project(":common:utils-java").projectDir = file("common/utils-java")
project(":common:variant").projectDir = file("common/variant")
project(":common:tags").projectDir = file("common/tags")
project(":common:network-yarn").projectDir = file("common/network-yarn")

project(":sql:connect:shims").projectDir = file("sql/connect/shims")
project(":sql:api").projectDir = file("sql/api")
project(":sql:catalyst").projectDir = file("sql/catalyst")
project(":sql:core").projectDir = file("sql/core")
project(":sql:hive").projectDir = file("sql/hive")
project(":sql:pipelines").projectDir = file("sql/pipelines")
project(":sql:connect:server").projectDir = file("sql/connect/server")
project(":sql:connect:common").projectDir = file("sql/connect/common")
project(":sql:connect:client:jvm").projectDir = file("sql/connect/client/jvm")
project(":sql:hive-thriftserver").projectDir = file("sql/hive-thriftserver")

project(":connector:kafka-0-10-token-provider").projectDir = file("connector/kafka-0-10-token-provider")
project(":connector:kafka-0-10").projectDir = file("connector/kafka-0-10")
project(":connector:kafka-0-10-assembly").projectDir = file("connector/kafka-0-10-assembly")
project(":connector:kafka-0-10-sql").projectDir = file("connector/kafka-0-10-sql")
project(":connector:avro").projectDir = file("connector/avro")
project(":connector:protobuf").projectDir = file("connector/protobuf")
project(":connector:spark-ganglia-lgpl").projectDir = file("connector/spark-ganglia-lgpl")
project(":connector:kinesis-asl").projectDir = file("connector/kinesis-asl")
project(":connector:kinesis-asl-assembly").projectDir = file("connector/kinesis-asl-assembly")
project(":connector:docker-integration-tests").projectDir = file("connector/docker-integration-tests")
project(":connector:profiler").projectDir = file("connector/profiler")

project(":resource-managers:yarn").projectDir = file("resource-managers/yarn")
project(":resource-managers:kubernetes:core").projectDir = file("resource-managers/kubernetes/core")
project(":resource-managers:kubernetes:integration-tests").projectDir = file("resource-managers/kubernetes/integration-tests")

project(":hadoop-cloud").projectDir = file("hadoop-cloud")