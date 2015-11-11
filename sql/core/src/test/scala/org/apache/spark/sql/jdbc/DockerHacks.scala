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
import scala.collection.mutable.MutableList

import com.spotify.docker.client.messages.ContainerConfig
import com.spotify.docker.client._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.BeforeAndAfterAll

abstract class DatabaseOnDocker {
  /**
   * The docker image to be pulled
   */
  def imageName: String

  /**
   * A Seq of environment variables in the form of VAR=value
   */
  def env: Seq[String]

  /**
   * jdbcUrl should be a lazy val or a function since `ip` it relies on is only available after
   * the docker container starts
   */
  def jdbcUrl: String

  private val docker: DockerClient = DockerClientFactory.get()
  private var containerId: String = null

  lazy val ip = docker.inspectContainer(containerId).networkSettings.ipAddress

  def start(): Unit = {
    while (true) {
      try {
        val config = ContainerConfig.builder()
          .image(imageName).env(env.asJava)
          .build()
        containerId = docker.createContainer(config).id
        docker.startContainer(containerId)
        return
      } catch {
        case e: ImageNotFoundException => retry(5)(docker.pull(imageName))
      }
    }
  }

  private def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e if n > 1 =>
        retry(n - 1)(fn)
    }
  }

  def close(): Unit = {
    docker.killContainer(containerId)
    docker.removeContainer(containerId)
    DockerClientFactory.close(docker)
  }
}

abstract class DatabaseIntegrationSuite extends SparkFunSuite
  with BeforeAndAfterAll with SharedSQLContext {

  def db: DatabaseOnDocker

  def waitForDatabase(ip: String, maxMillis: Long) {
    val before = System.currentTimeMillis()
    var lastException: java.sql.SQLException = null
    while (true) {
      if (System.currentTimeMillis() > before + maxMillis) {
        throw new java.sql.SQLException(s"Database not up after $maxMillis ms.", lastException)
      }
      try {
        val conn = java.sql.DriverManager.getConnection(db.jdbcUrl)
        conn.close()
        return
      } catch {
        case e: java.sql.SQLException =>
          lastException = e
          java.lang.Thread.sleep(250)
      }
    }
  }

  def setupDatabase(ip: String): Unit = {
    val conn: Connection = java.sql.DriverManager.getConnection(db.jdbcUrl)
    try {
      dataPreparation(conn)
    } finally {
      conn.close()
    }
  }

  /**
   * Prepare databases and tables for testing
   */
  def dataPreparation(connection: Connection)

  override def beforeAll() {
    super.beforeAll()
    db.start()
    waitForDatabase(db.ip, 60000)
    setupDatabase(db.ip)
  }

  override def afterAll() {
    try {
      db.close()
    } finally {
      super.afterAll()
    }
  }
}

/**
 * A factory and morgue for DockerClient objects.  In the DockerClient we use,
 * calling close() closes the desired DockerClient but also renders all other
 * DockerClients inoperable.  This is inconvenient if we have more than one
 * open, such as during tests.
 */
object DockerClientFactory {
  var numClients: Int = 0
  val zombies = new MutableList[DockerClient]()

  def get(): DockerClient = {
    this.synchronized {
      numClients = numClients + 1
      DefaultDockerClient.fromEnv.build()
    }
  }

  def close(dc: DockerClient) {
    this.synchronized {
      numClients = numClients - 1
      zombies += dc
      if (numClients == 0) {
        zombies.foreach(_.close())
        zombies.clear()
      }
    }
  }
}
