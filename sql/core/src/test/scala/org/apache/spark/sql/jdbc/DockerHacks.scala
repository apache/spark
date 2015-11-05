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

import java.sql.Connection

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.spotify.docker.client.messages.ContainerConfig
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
   * Return a JDBC URL that connects to the database running at the given IP address.
   */
  def getJdbcUrl(ip: String): String
}

abstract class DatabaseIntegrationSuite
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
      // Ensure that the Docker image is installed:
      try {
        docker.inspectImage(db.imageName)
      } catch {
        case e: ImageNotFoundException =>
          log.warn(s"Docker image ${db.imageName} not found; pulling image from registry")
          docker.pull(db.imageName)
      }
      // Launch the container:
      val config = ContainerConfig.builder()
        .image(db.imageName)
        .env(db.env.map { case (k, v) => s"$k=$v" }.toSeq.asJava)
        .build()
      containerId = docker.createContainer(config).id
      docker.startContainer(containerId)
      // Wait until the database has started and is accepting JDBC connections:
      jdbcUrl = db.getJdbcUrl(ip = docker.inspectContainer(containerId).networkSettings.ipAddress)
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
        } finally {
          docker.close()
        }
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Prepare databases and tables for testing.
   */
  def dataPreparation(connection: Connection): Unit
}
