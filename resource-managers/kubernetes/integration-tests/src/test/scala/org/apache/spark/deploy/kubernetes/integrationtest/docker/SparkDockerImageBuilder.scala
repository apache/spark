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

import java.io.File
import java.net.URI
import java.nio.file.Paths

import scala.collection.JavaConverters._

import com.spotify.docker.client.{DefaultDockerClient, DockerCertificates, LoggingBuildHandler}
import org.apache.http.client.utils.URIBuilder
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}

import org.apache.spark.internal.Logging
import org.apache.spark.util.RedirectThread



private[spark] class SparkDockerImageBuilder
  (private val dockerEnv: Map[String, String]) extends Logging{

  private val DOCKER_BUILD_PATH = Paths.get("target", "docker")
  // Dockerfile paths must be relative to the build path.
  private val BASE_DOCKER_FILE = "dockerfiles/spark-base/Dockerfile"
  private val DRIVER_DOCKER_FILE = "dockerfiles/driver/Dockerfile"
  private val DRIVERPY_DOCKER_FILE = "dockerfiles/driver-py/Dockerfile"
  private val EXECUTOR_DOCKER_FILE = "dockerfiles/executor/Dockerfile"
  private val EXECUTORPY_DOCKER_FILE = "dockerfiles/executor-py/Dockerfile"
  private val SHUFFLE_SERVICE_DOCKER_FILE = "dockerfiles/shuffle-service/Dockerfile"
  private val INIT_CONTAINER_DOCKER_FILE = "dockerfiles/init-container/Dockerfile"
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
    // Building Python distribution environment
    val pythonExec = sys.env.get("PYSPARK_DRIVER_PYTHON")
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("/usr/bin/python")
    val builder = new ProcessBuilder(
      Seq(pythonExec, "setup.py", "sdist").asJava)
    builder.directory(new File(DOCKER_BUILD_PATH.toFile, "python"))
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    val process = builder.start()
    new RedirectThread(process.getInputStream, System.out, "redirect output").start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      logInfo(s"exitCode: $exitCode")
    }
    buildImage("spark-base", BASE_DOCKER_FILE)
    buildImage("spark-driver", DRIVER_DOCKER_FILE)
    buildImage("spark-driver-py", DRIVERPY_DOCKER_FILE)
    buildImage("spark-executor", EXECUTOR_DOCKER_FILE)
    buildImage("spark-executor-py", EXECUTORPY_DOCKER_FILE)
    buildImage("spark-shuffle", SHUFFLE_SERVICE_DOCKER_FILE)
    buildImage("spark-resource-staging-server", STAGING_SERVER_DOCKER_FILE)
    buildImage("spark-init", INIT_CONTAINER_DOCKER_FILE)
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
