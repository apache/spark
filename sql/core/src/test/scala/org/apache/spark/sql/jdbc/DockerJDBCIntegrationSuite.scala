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

import java.net.{InetAddress, Inet4Address, NetworkInterface, ServerSocket}
import java.sql.Connection

import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.Try
import scala.util.control.NonFatal

import com.spotify.docker.client.messages.{ContainerConfig, HostConfig, PortBinding}
import com.spotify.docker.client._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

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
   * The container-internal JDBC port that the database listens on.
   */
  val jdbcPort: Int

  /**
   * Return a JDBC URL that connects to the database running at the given IP address and port.
   */
  def getJdbcUrl(ip: String, port: Int): String
}

abstract class DockerJDBCIntegrationSuite
  extends SparkFunSuite
  with BeforeAndAfterAll
  with Eventually
  with SharedSQLContext {

  val db: DatabaseOnDocker

  private var docker: DockerClient = _
  private var containerId: String = _
  protected var jdbcUrl: String = _

  override def beforeAll() {
    super.beforeAll()
    try {
      docker = DefaultDockerClient.fromEnv.build()
      // Check that Docker is actually up
      try {
        docker.ping()
      } catch {
        case NonFatal(e) =>
          log.error("Exception while connecting to Docker. Check whether Docker is running.")
          throw e
      }
      // Ensure that the Docker image is installed:
      try {
        docker.inspectImage(db.imageName)
      } catch {
        case e: ImageNotFoundException =>
          log.warn(s"Docker image ${db.imageName} not found; pulling image from registry")
          docker.pull(db.imageName)
      }
      // Configure networking (necessary for boot2docker / Docker Machine)
      val externalPort: Int = {
        val sock = new ServerSocket(0)
        val port = sock.getLocalPort
        sock.close()
        port
      }
      val dockerIp = getDockerIp()
      val hostConfig: HostConfig = HostConfig.builder()
        .networkMode("bridge")
        .portBindings(
          Map(s"${db.jdbcPort}/tcp" -> List(PortBinding.of(dockerIp, externalPort)).asJava).asJava)
        .build()
      // Create the database container:
      val config = ContainerConfig.builder()
        .image(db.imageName)
        .networkDisabled(false)
        .env(db.env.map { case (k, v) => s"$k=$v" }.toSeq.asJava)
        .hostConfig(hostConfig)
        .exposedPorts(s"${db.jdbcPort}/tcp")
        .build()
      containerId = docker.createContainer(config).id
      // Start the container and wait until the database can accept JDBC connections:
      docker.startContainer(containerId)
      jdbcUrl = db.getJdbcUrl(dockerIp, externalPort)
      eventually(timeout(60.seconds), interval(1.seconds)) {
        val conn = java.sql.DriverManager.getConnection(jdbcUrl)
        conn.close()
      }
      // Run any setup queries:
      val conn: Connection = java.sql.DriverManager.getConnection(jdbcUrl)
      try {
        dataPreparation(conn)
      } finally {
        conn.close()
      }
    } catch {
      case NonFatal(e) =>
        try {
          afterAll()
        } finally {
          throw e
        }
    }
  }

  override def afterAll() {
    try {
      if (docker != null) {
        try {
          if (containerId != null) {
            docker.killContainer(containerId)
            docker.removeContainer(containerId)
          }
        } catch {
          case NonFatal(e) =>
            logWarning(s"Could not stop container $containerId", e)
        } finally {
          docker.close()
        }
      }
    } finally {
      super.afterAll()
    }
  }

  private def getDockerIp(): String = {
    /** If docker-machine is setup on this box, attempts to find the ip from it. */
    def findFromDockerMachine(): Option[String] = {
      sys.env.get("DOCKER_MACHINE_NAME").flatMap { name =>
        Try(Seq("/bin/bash", "-c", s"docker-machine ip $name 2>/dev/null").!!.trim).toOption
      }
    }
    sys.env.get("DOCKER_IP")
      .orElse(findFromDockerMachine())
      .orElse(Try(Seq("/bin/bash", "-c", "boot2docker ip 2>/dev/null").!!.trim).toOption)
      .getOrElse {
        // This block of code is based on Utils.findLocalInetAddress(), but is modified to blacklist
        // certain interfaces.
        val address = InetAddress.getLocalHost
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val blackListedIFs = Seq(
          "vboxnet0",  // Mac
          "docker0"    // Linux
        )
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq.filter { i =>
          !blackListedIFs.contains(i.getName)
        }
        val reOrderedNetworkIFs = activeNetworkIFs.reverse
        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            return strippedAddress.getHostAddress
          }
        }
        address.getHostAddress
      }
  }

  /**
   * Prepare databases and tables for testing.
   */
  def dataPreparation(connection: Connection): Unit
}
