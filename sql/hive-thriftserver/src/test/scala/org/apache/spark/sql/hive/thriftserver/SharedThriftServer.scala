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

package org.apache.spark.sql.hive.thriftserver

import java.io.File
import java.sql.{DriverManager, Statement}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.cli.thrift.ThriftCLIService

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait SharedThriftServer extends SharedSparkSession {
  Utils.classForName(classOf[HiveDriver].getCanonicalName)

  protected var hiveServer2: HiveThriftServer2 = _
  protected var serverPort: Int = 0
  protected val hiveConfList = "a=avalue;b=bvalue"
  protected val hiveVarList = "c=cvalue;d=dvalue"

  private def getTempDir(): File = {
    val file = Utils.createTempDir()
    file.delete()
    file
  }

  protected val operationLogPath: File = getTempDir()
  protected val lScratchDir: File = getTempDir()
  protected val metastorePath: File = getTempDir()

  protected def extraConf: Map[String, String] = Map.empty
  protected def user: String = System.getProperty("user.name")

  def mode: ServerMode.Value = ServerMode.binary

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.CATALOG_IMPLEMENTATION, "hive")
      .set("spark.hadoop." + ConfVars.METASTORECONNECTURLKEY.varname,
        s"jdbc:derby:;databaseName=$metastorePath;create=true")
      .set("spark.ui.enabled", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(0))) { case (started, attempt) =>
      started.orElse(Try(startThriftServer(attempt)))
    }.recover {
      case cause: Throwable =>
        throw cause
    }.get
    logInfo("HiveThriftServer2 started successfully")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    try {
      if (hiveServer2 != null) {
        hiveServer2.stop()
        hiveServer2 = null
      }
    } finally {
      Utils.deleteRecursively(operationLogPath)
      Utils.deleteRecursively(lScratchDir)
      Utils.deleteRecursively(metastorePath)
    }
  }

  private def jdbcUri: String = if (mode == ServerMode.http) {
    s"""jdbc:hive2://localhost:$serverPort/
       |default?
       |hive.server2.transport.mode=http;
       |hive.server2.thrift.http.path=cliservice;
       |$hiveConfList#$hiveVarList
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"""jdbc:hive2://localhost:$serverPort/?$hiveConfList#$hiveVarList"""
  }

  def withMultipleConnectionJdbcStatement(tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      tableNames.foreach { name =>
        // TODO: Need a better way to drop the view.
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          statements.head.execute(s"DROP VIEW IF EXISTS $name")
        } else {
          statements.head.execute(s"DROP TABLE IF EXISTS $name")
        }
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withDatabase(dbNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.foreach { name =>
        statements.head.execute(s"DROP DATABASE IF EXISTS $name")
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }

  private def startThriftServer(attempt: Int): Unit = {
    logInfo(s"Trying to start HiveThriftServer2: mode: $mode, attempt=$attempt")
    val sqlContext = spark.newSession().sqlContext
    // Set the HIVE_SERVER2_THRIFT_PORT to 0, so it could randomly pick any free port to use.
    // It's much more robust than set a random port generated by ourselves ahead
    sqlContext.setConf(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, "0")
    sqlContext.setConf(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname, "0")
    sqlContext.setConf(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, mode.toString)
    sqlContext.setConf(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION.varname,
      operationLogPath.getAbsolutePath)
    sqlContext.setConf(ConfVars.LOCALSCRATCHDIR.varname, lScratchDir.getAbsolutePath)
    hiveServer2 = HiveThriftServer2.startWithContext(sqlContext)
    hiveServer2.getServices.asScala.foreach {
      case t: ThriftCLIService if t.getPortNumber != 0 =>
        serverPort = t.getPortNumber
        logInfo(s"Started HiveThriftServer2: mode: $mode port=$serverPort, attempt=$attempt")
      case _ =>
    }

    // Wait for thrift server to be ready to serve the query, via executing simple query
    // till the query succeeds. See SPARK-30345 for more details.
    eventually(timeout(30.seconds), interval(1.seconds)) {
      withJdbcStatement() { _.execute("SELECT 1") }
    }
  }
}
