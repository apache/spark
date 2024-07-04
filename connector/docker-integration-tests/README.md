---
layout: global
title: Spark Docker Integration Tests
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Running the Docker integration tests

Note that the integration test framework is using thirdparty Docker images to test functionalities
of integration with other data sources, not running Spark itself in Docker containers, which is
actually done by the Kubernetes Integration Tests module.

In order to run the Docker integration tests, the [Docker engine](https://docs.docker.com/engine/installation/)
needs to be installed and started on the machine. Additionally, the environment variable
`ENABLE_DOCKER_INTEGRATION_TESTS=1` shall be specified to enable the Docker integration tests.

    export ENABLE_DOCKER_INTEGRATION_TESTS=1

or,

    ENABLE_DOCKER_INTEGRATION_TESTS=1 ./build/sbt -Pdocker-integration-tests "docker-integration-tests/test"

## Running an individual Docker integration test

Testing the whole module of Docker Integration Tests might be time-consuming because of image pulling and
the container bootstrapping. To run an individual Docker integration test, use the following command:

    ./build/sbt -Pdocker-integration-tests "docker-integration-tests/testOnly <test class name>"

## Using a custom Docker image

Besides the default Docker images, the integration tests can be run with custom Docker images. For example,

    ORACLE_DOCKER_IMAGE_NAME=gvenzl/oracle-free:23.4-slim-faststart ./build/sbt -Pdocker-integration-tests "docker-integration-tests/testOnly *OracleIntegrationSuite"

The following environment variables can be used to specify the custom Docker images for different databases:

- DB2_DOCKER_IMAGE_NAME
- MARIADB_DOCKER_IMAGE_NAME
- MSSQLSERVER_DOCKER_IMAGE_NAME
- MYSQL_DOCKER_IMAGE_NAME
- ORACLE_DOCKER_IMAGE_NAME
- POSTGRES_DOCKER_IMAGE_NAME

## Using a custom Docker context

In certain scenarios, you may want to use a different docker context/endpoint instead of the default provided by
Docker Desktop.

    docker context ls
    NAME                TYPE                DESCRIPTION                               DOCKER ENDPOINT                 KUBERNETES ENDPOINT   ORCHESTRATOR
    default *           moby                Current DOCKER_HOST based configuration   unix:///var/run/docker.sock
    desktop-linux       moby                Docker Desktop                            unix:///Users/.../docker.sock

    docker context use desktop-linux

Then you can run the integration tests as usual targeting the Docker endpoint named `desktop-linux`.

This is useful when the upstream Docker image can not starton the Docker Desktop. For example, when the image you
use only supports `x86_64` architecture, but you are running on an Apple Silicon `aarch64` machine, for which case
you may want to use a custom context that is able to mock a `x86_64` architecture, such as `colima`. After
[colima](https://github.com/abiosoft/colima) installed, you can start a runtime with `x86_64` support and run the
integration tests as follows:

    colima start --arch x86_64 --memory 8 --network-address
    docker context colima
    ./build/sbt -Pdocker-integration-tests "docker-integration-tests/testOnly *OracleIntegrationSuite"

## Available Properties

The following are the available properties that can be passed to optimize testing experience.

    ./build/sbt -Pdocker-integration-tests \
                -Dspark.test.docker.keepContainer=true \
                "testOnly *MariaDBKrbIntegrationSuite"

<table>
  <tr>
    <th>Property</th>
    <th>Description</th>
    <th>Default</th>
  </tr>
  <tr>
    <td><code>spark.test.docker.keepContainer</code></td>
    <td>
      When true, the Docker container used for the test will keep running after all tests finished in a single test file.
    </td>
    <td>false</td>
  </tr>
  <tr>
    <td><code>spark.test.docker.removePulledImage</code></td>
    <td>
      When true, the Docker image used for the test will not be preserved after all tests finished in a single test file.
    </td>
    <td>true</td>
  </tr>
  <tr>
    <td><code>spark.test.docker.imagePullTimeout</code></td>
    <td>
      Timeout for pulling the Docker image before the tests start.
    </td>
    <td>5min</td>
  </tr>
  <tr>
    <td><code>spark.test.docker.startContainerTimeout</code></td>
    <td>
      Timeout for container to spin up.
    </td>
    <td>5min</td>
  </tr>
  <tr>
    <td><code>spark.test.docker.connectionTimeout</code></td>
    <td>
      Timeout for connecting the inner service in the container, such as JDBC services.
    </td>
    <td>5min(might get overridden by some inherits)</td>
  </tr>
</table>
