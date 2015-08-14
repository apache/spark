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

import java.sql.DriverManager
import java.util.Properties

import org.scalatest.BeforeAndAfterAll

import com.spotify.docker.client.{ImageNotFoundException, DockerClient}
import com.spotify.docker.client.messages.ContainerConfig

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class PostgresDatabase {
  val docker: DockerClient = DockerClientFactory.get()
  var containerId: String = null

  start()

  def start(): Unit = {
    while (true) {
      try {
        val config = ContainerConfig.builder()
          .image("postgres").env("POSTGRES_PASSWORD=rootpass")
          .build()
        containerId = docker.createContainer(config).id
        docker.startContainer(containerId)
        return
      } catch {
        case e: ImageNotFoundException => retry(3)(docker.pull("postgres"))
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

  lazy val ip = docker.inspectContainer(containerId).networkSettings.ipAddress

  def close(): Unit = {
    docker.killContainer(containerId)
    docker.removeContainer(containerId)
    DockerClientFactory.close(docker)
  }
}

class PostgresIntegrationSuite extends SparkFunSuite with BeforeAndAfterAll with SharedSQLContext {
  lazy val db = new PostgresDatabase()

  def url(ip: String): String =
    s"jdbc:postgresql://$ip:5432/postgres?user=postgres&password=rootpass"

  def waitForDatabase(ip: String, maxMillis: Long) {
    val before = System.currentTimeMillis()
    var lastException: java.sql.SQLException = null
    while (true) {
      if (System.currentTimeMillis() > before + maxMillis) {
        throw new java.sql.SQLException(s"Database not up after $maxMillis ms.",
          lastException)
      }
      try {
        val conn = java.sql.DriverManager.getConnection(url(ip))
        conn.close()
        return
      } catch {
        case e: java.sql.SQLException =>
          lastException = e
          java.lang.Thread.sleep(250)
      }
    }
  }

  def setupDatabase(ip: String) {
    val conn = DriverManager.getConnection(url(ip))
    try {
      conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
      conn.setCatalog("foo")
      conn.prepareStatement("CREATE TABLE bar (a text, b integer, c double precision, d bigint, "
        + "e bit(1), f bit(10), g bytea, h boolean, i inet, j cidr)").executeUpdate()
      conn.prepareStatement("INSERT INTO bar VALUES ('hello', 42, 1.25, 123456789012345, B'0', "
        + "B'1000100101', E'\\\\xDEADBEEF', true, '172.16.0.42', '192.168.0.0/16')").executeUpdate()
    } finally {
      conn.close()
    }
  }

  override def beforeAll() {
    super.beforeAll()
    waitForDatabase(db.ip, 60000)
    setupDatabase(db.ip)
  }

  override def afterAll() {
    db.close()
  }

  test("Type mapping for various types") {
    val df = sqlContext.read.jdbc(url(db.ip), "public.bar", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 10)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.Integer"))
    assert(types(2).equals("class java.lang.Double"))
    assert(types(3).equals("class java.lang.Long"))
    assert(types(4).equals("class java.lang.Boolean"))
    assert(types(5).equals("class [B"))
    assert(types(6).equals("class [B"))
    assert(types(7).equals("class java.lang.Boolean"))
    assert(types(8).equals("class java.lang.String"))
    assert(types(9).equals("class java.lang.String"))
    assert(rows(0).getString(0).equals("hello"))
    assert(rows(0).getInt(1) == 42)
    assert(rows(0).getDouble(2) == 1.25)
    assert(rows(0).getLong(3) == 123456789012345L)
    assert(rows(0).getBoolean(4) == false)
    // BIT(10)'s come back as ASCII strings of ten ASCII 0's and 1's...
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](5),
      Array[Byte](49, 48, 48, 48, 49, 48, 48, 49, 48, 49)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](6),
      Array[Byte](0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte)))
    assert(rows(0).getBoolean(7) == true)
    assert(rows(0).getString(8) == "172.16.0.42")
    assert(rows(0).getString(9) == "192.168.0.0/16")
  }

  test("Basic write test") {
    val df = sqlContext.read.jdbc(url(db.ip), "public.bar", new Properties)
    df.write.jdbc(url(db.ip), "public.barcopy", new Properties)
    // Test only that it doesn't bomb out.
  }
}
