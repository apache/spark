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
package org.apache.spark.sql.test

import java.io.{File, IOException, OutputStream}
import java.lang.ProcessBuilder.Redirect
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkBuildInfo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.RetryPolicy
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.sql.test.IntegrationTestUtils._
import org.apache.spark.util.ArrayImplicits._

/**
 * An util class to start a local spark connect server in a different process for local E2E tests.
 * Pre-running the tests, the spark connect artifact needs to be built using e.g. `build/sbt
 * package`. It is designed to start the server once but shared by all tests.
 *
 * Set system property `spark.test.home` or env variable `SPARK_HOME` if the test is not executed
 * from the Spark project top folder. Set system property `spark.debug.sc.jvm.client=true` or
 * environment variable `SPARK_DEBUG_SC_JVM_CLIENT=true` to print the server process output in the
 * console to debug server start stop problems.
 */
object SparkConnectServerUtils {

  // The equivalent command to start the connect server via command line:
  // bin/spark-shell --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

  // Server port
  val port: Int =
    ConnectCommon.CONNECT_GRPC_BINDING_PORT + util.Random.nextInt(1000)

  @volatile private var stopped = false

  private var consoleOut: OutputStream = _
  private val serverStopCommand = "q"

  private lazy val sparkConnect: java.lang.Process = {
    debug("Starting the Spark Connect Server...")
    val connectJar =
      findJar("sql/connect/server", "spark-connect-assembly", "spark-connect").getCanonicalPath

    val command = Seq.newBuilder[String]
    command += "bin/spark-submit"
    command += "--driver-class-path" += connectJar
    command += "--class" += "org.apache.spark.sql.connect.SimpleSparkConnectService"
    command += "--conf" += s"spark.connect.grpc.binding.port=$port"
    command ++= testConfigs
    command ++= debugConfigs
    command += connectJar
    val builder = new ProcessBuilder(command.result(): _*)
    builder.directory(new File(sparkHome))
    val environment = builder.environment()
    environment.remove("SPARK_DIST_CLASSPATH")
    if (isDebug) {
      builder.redirectError(Redirect.INHERIT)
      builder.redirectOutput(Redirect.INHERIT)
    }

    val process = builder.start()
    consoleOut = process.getOutputStream

    // Adding JVM shutdown hook
    sys.addShutdownHook(stop())
    process
  }

  /**
   * As one shared spark will be started for all E2E tests, for tests that needs some special
   * configs, we add them here
   */
  private def testConfigs: Seq[String] = {
    // To find InMemoryTableCatalog for V2 writer tests
    val catalystTestJar =
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
      // SPARK-43647: Proactively cleaning the `classes` and `test-classes` dir of hive
      // module to avoid unexpected loading of `DataSourceRegister` in hive module during
      // testing without `-Phive` profile.
      IntegrationTestUtils.cleanUpHiveClassesDirIfNeeded()
      "in-memory"
    }
    val confs = Seq(
      // Use InMemoryTableCatalog for V2 writer tests
      "spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog",
      // Try to use the hive catalog, fallback to in-memory if it is not there.
      "spark.sql.catalogImplementation=" + catalogImplementation,
      // Make the server terminate reattachable streams every 1 second and 123 bytes,
      // to make the tests exercise reattach.
      "spark.connect.execute.reattachable.senderMaxStreamDuration=1s",
      "spark.connect.execute.reattachable.senderMaxStreamSize=123",
      // Testing SPARK-49673, setting maxBatchSize to 10MiB
      s"spark.connect.grpc.arrow.maxBatchSize=${10*1024*1024}",
      // Disable UI
      "spark.ui.enabled=false")
    Seq("--jars", catalystTestJar) ++ confs.flatMap(v => "--conf" :: v :: Nil)
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
      if (!sparkConnect.waitFor(2, TimeUnit.SECONDS)) {
        sparkConnect.destroyForcibly().waitFor(2, TimeUnit.SECONDS)
      }
      val code = sparkConnect.exitValue()
      debug(s"Spark Connect Server is stopped with exit code: $code")
      code
    } catch {
      case e: IOException if e.getMessage.contains("Stream closed") =>
        -1
      case e: Throwable =>
        debug(e)
        sparkConnect.destroyForcibly()
        throw e
    }
  }

  def syncTestDependencies(spark: SparkSession): Unit = {
    // Both SBT & Maven pass the test-classes as a directory instead of a jar.
    val testClassesPath = Paths.get(IntegrationTestUtils.connectClientTestClassDir)
    spark.client.artifactManager.addClassDir(testClassesPath)

    // We need scalatest & scalactic on the session's classpath to make the tests work.
    val jars = System
      .getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter { e: String =>
        val fileName = e.substring(e.lastIndexOf(File.separatorChar) + 1)
        fileName.endsWith(".jar") &&
        (fileName.startsWith("scalatest") || fileName.startsWith("scalactic"))
      }
      .map(e => Paths.get(e).toUri)
    spark.client.artifactManager.addArtifacts(jars.toImmutableArraySeq)
  }

  def createSparkSession(): SparkSession = {
    SparkConnectServerUtils.start()

    val spark = SparkSession
      .builder()
      .client(
        SparkConnectClient
          .builder()
          .userId("test")
          .port(port)
          .retryPolicy(RetryPolicy
            .defaultPolicy()
            .copy(maxRetries = Some(10), maxBackoff = Some(FiniteDuration(30, "s"))))
          .build())
      .create()

    // Execute an RPC which will get retried until the server is up.
    eventually(timeout(1.minute)) {
      assert(spark.version == SparkBuildInfo.spark_version)
    }

    // Auto-sync dependencies.
    SparkConnectServerUtils.syncTestDependencies(spark)

    spark
  }
}

trait RemoteSparkSession extends BeforeAndAfterAll { self: Suite =>
  import SparkConnectServerUtils._
  var spark: SparkSession = _
  protected lazy val serverPort: Int = port

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = createSparkSession()
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
}
