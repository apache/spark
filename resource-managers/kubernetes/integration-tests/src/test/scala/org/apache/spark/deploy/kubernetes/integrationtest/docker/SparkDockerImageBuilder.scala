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
package org.apache.spark.deploy.kubernetes.integrationtest.docker

import java.net.URI
import java.nio.file.Paths

import com.spotify.docker.client.{DefaultDockerClient, DockerCertificates, LoggingBuildHandler}
import org.apache.http.client.utils.URIBuilder
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}

private[spark] class SparkDockerImageBuilder(private val dockerEnv: Map[String, String]) {

  private val DOCKER_BUILD_PATH = Paths.get("target", "docker")
  // Dockerfile paths must be relative to the build path.
  private val DRIVER_V1_DOCKER_FILE = "dockerfiles/driver/Dockerfile"
  private val DRIVER_V2_DOCKER_FILE = "dockerfiles/driver-v2/Dockerfile"
  private val EXECUTOR_DOCKER_FILE = "dockerfiles/executor/Dockerfile"
  private val SHUFFLE_SERVICE_DOCKER_FILE = "dockerfiles/shuffle-service/Dockerfile"
  private val DRIVER_INIT_DOCKER_FILE = "dockerfiles/driver-init/Dockerfile"
  private val STAGING_SERVER_DOCKER_FILE = "dockerfiles/resource-staging-server/Dockerfile"
  private val STATIC_ASSET_SERVER_DOCKER_FILE =
    "dockerfiles/integration-test-asset-server/Dockerfile"
  private val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  private val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  private val dockerHost = dockerEnv.getOrElse("DOCKER_HOST",
      throw new IllegalStateException("DOCKER_HOST env not found."))

  private val originalDockerUri = URI.create(dockerHost)
  private val httpsDockerUri = new URIBuilder()
      .setHost(originalDockerUri.getHost)
      .setPort(originalDockerUri.getPort)
      .setScheme("https")
      .build()

  private val dockerCerts = dockerEnv.getOrElse("DOCKER_CERT_PATH",
      throw new IllegalStateException("DOCKER_CERT_PATH env not found."))

  private val dockerClient = new DefaultDockerClient.Builder()
    .uri(httpsDockerUri)
    .dockerCertificates(DockerCertificates
        .builder()
        .dockerCertPath(Paths.get(dockerCerts))
        .build().get())
    .build()

  def buildSparkDockerImages(): Unit = {
    Eventually.eventually(TIMEOUT, INTERVAL) { dockerClient.ping() }
    buildImage("spark-driver", DRIVER_V1_DOCKER_FILE)
    buildImage("spark-executor", EXECUTOR_DOCKER_FILE)
    buildImage("spark-shuffle", SHUFFLE_SERVICE_DOCKER_FILE)
    buildImage("spark-driver-v2", DRIVER_V2_DOCKER_FILE)
    buildImage("spark-resource-staging-server", STAGING_SERVER_DOCKER_FILE)
    buildImage("spark-driver-init", DRIVER_INIT_DOCKER_FILE)
    buildImage("spark-integration-test-asset-server", STATIC_ASSET_SERVER_DOCKER_FILE)
  }

  private def buildImage(name: String, dockerFile: String): Unit = {
    dockerClient.build(
      DOCKER_BUILD_PATH,
      name,
      dockerFile,
      new LoggingBuildHandler())
  }
}
