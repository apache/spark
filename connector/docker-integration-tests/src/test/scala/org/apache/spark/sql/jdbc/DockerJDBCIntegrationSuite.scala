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

package org.apache.spark.sql.jdbc

import java.net.ServerSocket
import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.concurrent.TimeoutException
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.{ResultCallback, ResultCallbackTemplate}
import com.github.dockerjava.api.command.{CreateContainerResponse, PullImageResultCallback}
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model._
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.LogKey.{CLASS_NAME, CONTAINER, STATUS}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{DockerUtils, Utils}
import org.apache.spark.util.Utils.timeStringAsSeconds

abstract class DatabaseOnDocker {
  /**
   * The docker image to be pulled.
   */
  val imageName: String

  /**
   * Environment variables to set inside of the Docker container while launching it.
   */
  val env: Map[String, String]

  /**
   * Whether or not to use ipc mode for shared memory when starting docker image
   */
  val usesIpc: Boolean

  /**
   * The container-internal JDBC port that the database listens on.
   */
  val jdbcPort: Int

  /**
   * Parameter whether the container should run privileged.
   */
  val privileged: Boolean = false

  /**
   * Return a JDBC URL that connects to the database running at the given IP address and port.
   */
  def getJdbcUrl(ip: String, port: Int): String

  /**
   * Return the JDBC properties needed for the connection.
   */
  def getJdbcProperties(): Properties = new Properties()

  /**
   * Optional entry point when container starts
   *
   * Startup process is a parameter of entry point. This may or may not be considered during
   * startup. Prefer entry point to startup process when you need a command always to be executed or
   * you want to change the initialization order.
   */
  def getEntryPoint: Option[String] = None

  /**
   * Optional process to run when container starts
   */
  def getStartupProcessName: Option[String] = None

  /**
   * Optional step before container starts
   */
  def beforeContainerStart(
      hostConfigBuilder: HostConfig,
      containerConfigBuilder: ContainerConfig): Unit = {}
}

abstract class DockerJDBCIntegrationSuite
  extends QueryTest with SharedSparkSession with Eventually with DockerIntegrationFunSuite {

  protected val dockerIp = DockerUtils.getDockerIp()
  val db: DatabaseOnDocker
  val keepContainer =
    sys.props.getOrElse("spark.test.docker.keepContainer", "false").toBoolean
  val removePulledImage =
    sys.props.getOrElse("spark.test.docker.removePulledImage", "true").toBoolean
  protected val imagePullTimeout: Long =
    timeStringAsSeconds(sys.props.getOrElse("spark.test.docker.imagePullTimeout", "5min"))
  protected val startContainerTimeout: Long =
    timeStringAsSeconds(sys.props.getOrElse("spark.test.docker.startContainerTimeout", "5min"))
  protected val connectionTimeout: PatienceConfiguration.Timeout = {
    val timeoutStr = sys.props.getOrElse("spark.test.docker.conn", "5min")
    timeout(timeStringAsSeconds(timeoutStr).seconds)
  }

  private var docker: DockerClient = _
  // Configure networking (necessary for boot2docker / Docker Machine)
  protected lazy val externalPort: Int = {
    val sock = new ServerSocket(0)
    val port = sock.getLocalPort
    sock.close()
    port
  }
  private var container: CreateContainerResponse = _
  private var pulled: Boolean = false
  protected var jdbcUrl: String = _

  override def beforeAll(): Unit = runIfTestsEnabled(s"Prepare for ${this.getClass.getName}") {
    super.beforeAll()
    try {
      val config = DefaultDockerClientConfig.createDefaultConfigBuilder.build
      val httpClient = new ZerodepDockerHttpClient.Builder()
        .dockerHost(config.getDockerHost)
        .sslConfig(config.getSSLConfig)
        .build()
      docker = DockerClientImpl.getInstance(config, httpClient)
      // Check that Docker is actually up
      try {
        docker.pingCmd().exec()
      } catch {
        case NonFatal(e) =>
          log.error("Exception while connecting to Docker. Check whether Docker is running.")
          throw e
      }
      try {
        // Ensure that the Docker image is installed:
        docker.inspectImageCmd(db.imageName).exec()
      } catch {
        case e: NotFoundException =>
          log.warn(s"Docker image ${db.imageName} not found; pulling image from registry")
          val callback = new PullImageResultCallback {
            override def onNext(item: PullResponseItem): Unit = {
              super.onNext(item)
              val status = item.getStatus
              if (status != null && status != "Downloading" && status != "Extracting") {
                logInfo(s"$status ${item.getId}")
              }
            }
          }

          val (success, time) = Utils.timeTakenMs(
            docker.pullImageCmd(db.imageName)
              .exec(callback)
              .awaitCompletion(imagePullTimeout, TimeUnit.SECONDS))

          if (success) {
            pulled = success
            logInfo(s"Successfully pulled image ${db.imageName} in $time ms")
          } else {
            throw new TimeoutException(
              s"Timeout('$imagePullTimeout secs') waiting for image ${db.imageName} to be pulled")
          }
      }

      val hostConfig = HostConfig
        .newHostConfig()
        .withNetworkMode("bridge")
        .withPrivileged(db.privileged)
        .withPortBindings(PortBinding.parse(s"$externalPort:${db.jdbcPort}"))

      if (db.usesIpc) {
        hostConfig.withIpcMode("host")
      }

      val containerConfig = new ContainerConfig()

      db.beforeContainerStart(hostConfig, containerConfig)

      // Create the database container:
      val createContainerCmd = docker.createContainerCmd(db.imageName)
        .withHostConfig(hostConfig)
        .withExposedPorts(ExposedPort.tcp(db.jdbcPort))
        .withEnv(db.env.map { case (k, v) => s"$k=$v" }.toList.asJava)
        .withNetworkDisabled(false)


      db.getEntryPoint.foreach(ep => createContainerCmd.withEntrypoint(ep))
      db.getStartupProcessName.foreach(n => createContainerCmd.withCmd(n))

      container = createContainerCmd.exec()
      // Start the container and wait until the database can accept JDBC connections:
      docker.startContainerCmd(container.getId).exec()
      eventually(timeout(startContainerTimeout.seconds), interval(1.second)) {
        val response = docker.inspectContainerCmd(container.getId).exec()
        assert(response.getState.getRunning)
      }
      jdbcUrl = db.getJdbcUrl(dockerIp, externalPort)
      var conn: Connection = null
      eventually(connectionTimeout, interval(1.second)) {
        conn = getConnection()
      }
      // Run any setup queries:
      try {
        dataPreparation(conn)
      } finally {
        conn.close()
      }
    } catch {
      case NonFatal(e) =>
        logError(log"Failed to initialize Docker container for " +
          log"${MDC(CLASS_NAME, this.getClass.getName)}", e)
        try {
          afterAll()
        } finally {
          throw e
        }
    }
  }

  override def afterAll(): Unit = {
    try {
      cleanupContainer()
    } finally {
      if (docker != null) {
        docker.close()
      }
      super.afterAll()
    }
  }

  /**
   * Return the JDBC connection.
   */
  def getConnection(): Connection = {
    DriverManager.getConnection(jdbcUrl, db.getJdbcProperties())
  }

  /**
   * Prepare databases and tables for testing.
   */
  def dataPreparation(connection: Connection): Unit

  private def cleanupContainer(): Unit = {
    if (docker != null && container != null && !keepContainer) {
      try {
        docker.killContainerCmd(container.getId).exec()
      } catch {
        case NonFatal(e) =>
          val response = docker.inspectContainerCmd(container.getId).exec()
          logWarning(log"Container ${MDC(CONTAINER, container)} already stopped")
          val status = Option(response).map(_.getState.getStatus).getOrElse("unknown")
          logWarning(log"Could not stop container ${MDC(CONTAINER, container)} " +
            log"at stage '${MDC(STATUS, status)}'", e)
      } finally {
        logContainerOutput()
        docker.removeContainerCmd(container.getId).exec()
        if (removePulledImage && pulled) {
          docker.removeImageCmd(db.imageName).exec()
        }
      }
    }
  }

  private def logContainerOutput(): Unit = {
    logInfo("\n\n===== CONTAINER LOGS FOR container Id: " + container + " =====")
    docker.logContainerCmd(container.getId)
      .withStdOut(true)
      .withStdErr(true)
      .withFollowStream(true)
      .withSince(0).exec(
      new ResultCallbackTemplate[ResultCallback[Frame], Frame] {
        override def onNext(f: Frame): Unit = logInfo(f.toString)
      })
    logInfo("\n\n===== END OF CONTAINER LOGS FOR container Id: " + container + " =====")
  }
}
