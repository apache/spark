#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Get the root of the runtime repo
GIT_ROOT="$(git rev-parse --show-toplevel)"
echo "Root of repo is at $GIT_ROOT"

echo "Starting DB container.."
docker-compose --project-directory "${GIT_ROOT}/sql/core/src/test/scala/org/apache/spark/sql/crossdbms/bin" up --build --detach

echo "Waiting for DB container to be ready.."
# Change if using different container or name.
DOCKER_CONTAINER_NAME="postgres-db"
timeout 10s bash -c "until docker exec $DOCKER_CONTAINER_NAME pg_isready ; do sleep 1 ; done"

echo "Generating all golden files for postgres.."
SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite"

echo "Bringing down DB container.."
docker-compose --project-directory "${GIT_ROOT}/sql/core/src/test/scala/org/apache/spark/sql/crossdbms/bin" down --volumes
