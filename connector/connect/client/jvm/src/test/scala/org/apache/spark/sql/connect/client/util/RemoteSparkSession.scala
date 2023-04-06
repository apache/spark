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
package org.apache.spark.sql.connect.client.util

import java.io.{BufferedOutputStream, File}
import java.util.concurrent.TimeUnit

import scala.io.Source

import org.scalatest.BeforeAndAfterAll
import sys.process._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.client.util.IntegrationTestUtils._
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.util.Utils

/**
 * An util class to start a local spark connect server in a different process for local E2E tests.
 * Pre-running the tests, the spark connect artifact needs to be built using e.g. `build/sbt
 * package`. It is designed to start the server once but shared by all tests. It is equivalent to
 * use the following command to start the connect server via command line:
 *
 * {{{
 * bin/spark-shell \
 * --jars `ls connector/connect/server/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
 * --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
 * }}}
 *
 * Set system property `spark.test.home` or env variable `SPARK_HOME` if the test is not executed
 * from the Spark project top folder. Set system property `spark.debug.sc.jvm.client=true` to
 * print the server process output in the console to debug server start stop problems.
 */
object SparkConnectServerUtils {

  // Server port
  private[connect] val port = ConnectCommon.CONNECT_GRPC_BINDING_PORT + util.Random.nextInt(1000)

  @volatile private var stopped = false

  private var consoleOut: BufferedOutputStream = _
  private val serverStopCommand = "q"

  private lazy val sparkConnect: Process = {
    debug("Starting the Spark Connect Server...")
    val connectJar = findJar(
      "connector/connect/server",
      "spark-connect-assembly",
      "spark-connect").getCanonicalPath
    val driverClassPath = connectJar + ":" +
      findJar("sql/catalyst", "spark-catalyst", "spark-catalyst", test = true).getCanonicalPath
    val catalogImplementation = if (IntegrationTestUtils.isSparkHiveJarAvailable) {
      "hive"
    } else {
      // scalastyle:off println
      println(
        "Will start Spark Connect server with `spark.sql.catalogImplementation=in-memory`, " +
          "some tests that rely on Hive will be ignored. If you don't want to skip them:\n" +
          "1. Test with maven: run `build/mvn install -DskipTests -Phive` before testing\n" +
          "2. Test with sbt: run test with `-Phive` profile")
      // scalastyle:on println
      "in-memory"
    }
    val builder = Process(
      Seq(
        "bin/spark-submit",
        "--driver-class-path",
        driverClassPath,
        "--conf",
        s"spark.connect.grpc.binding.port=$port",
        "--conf",
        "spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog",
        "--conf",
        s"spark.sql.catalogImplementation=$catalogImplementation",
        "--class",
        "org.apache.spark.sql.connect.SimpleSparkConnectService",
        connectJar),
      new File(sparkHome))

    val io = new ProcessIO(
      in => consoleOut = new BufferedOutputStream(in),
      out => Source.fromInputStream(out).getLines.foreach(debug),
      err => Source.fromInputStream(err).getLines.foreach(debug))
    val process = builder.run(io)

    // Adding JVM shutdown hook
    sys.addShutdownHook(stop())
    process
  }

  def start(): Unit = {
    assert(!stopped)
    sparkConnect
  }

  def stop(): Int = {
    stopped = true
    debug("Stopping the Spark Connect Server...")
    try {
      consoleOut.write(serverStopCommand.getBytes)
      consoleOut.flush()
      consoleOut.close()
    } catch {
      case e: Throwable =>
        debug(e)
        sparkConnect.destroy()
    }

    val code = sparkConnect.exitValue()
    debug(s"Spark Connect Server is stopped with exit code: $code")
    code
  }
}

trait RemoteSparkSession extends ConnectFunSuite with BeforeAndAfterAll {
  import SparkConnectServerUtils._
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkConnectServerUtils.start()
    spark = SparkSession.builder().client(SparkConnectClient.builder().port(port).build()).build()

    // Retry and wait for the server to start
    val stop = System.nanoTime() + TimeUnit.MINUTES.toNanos(1) // ~1 min
    var sleepInternalMs = TimeUnit.SECONDS.toMillis(1) // 1s with * 2 backoff
    var success = false
    val error = new RuntimeException(s"Failed to start the test server on port $port.")

    while (!success && System.nanoTime() < stop) {
      try {
        // Run a simple query to verify the server is really up and ready
        val result = spark
          .sql("select val from (values ('Hello'), ('World')) as t(val)")
          .collect()
        assert(result.length == 2)
        success = true
        debug("Spark Connect Server is up.")
      } catch {
        // ignored the error
        case e: Throwable =>
          error.addSuppressed(e)
          Thread.sleep(sleepInternalMs)
          sleepInternalMs *= 2
      }
    }

    // Throw error if failed
    if (!success) {
      debug(error)
      throw error
    }
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } catch {
      case e: Throwable => debug(e)
    }
    spark = null
    super.afterAll()
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name").collect()
      }
    }
  }
}
