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

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.common.config.ConnectCommon

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite
import org.scalatest.BeforeAndAfterAll
import scala.sys.process._

/**
 * An util class to start a local spark connect server in a different process for local E2E tests.
 * It is designed to start the server once but shared by all tests. It is equivalent to use the
 * following command to start the connect server via command line: bin/spark-shell \
 * --jars `ls connector/connect/server/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
 * --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
 *
 * Set env variable `SPARK_HOME` if the test is not executed from the Spark project top folder.
 * Set env variable `DEBUG_SC_JVM_CLIENT=true` to print the server process output in the console
 * to debug server start stop problems.
 */
object SparkConnectServerUtils {
  // Env variables used for testing and debugging
  private val SPARK_HOME = "SPARK_HOME"
  private val ENV_DEBUG_SC_JVM_CLIENT = "DEBUG_SC_JVM_CLIENT"

  private val sparkHome = System.getProperty(SPARK_HOME, "./")
  private val isDebug = System.getProperty(ENV_DEBUG_SC_JVM_CLIENT, "false").toBoolean

  // Log server start stop debug info into console
  // scalastyle:off println
  private[connect] def debug(msg: String): Unit = if (isDebug) println(msg)
  // scalastyle:on println
  private[connect] def debug(error: Throwable): Unit = if (isDebug) error.printStackTrace()

  // Server port
  private[connect] val port = ConnectCommon.CONNECT_GRPC_BINDING_PORT

  @volatile private var stopped = false

  private lazy val sparkConnect: Process = {
    debug("Starting the Spark Connect Server...")
    val builder = Process(
      "bin/spark-shell",
      Seq(
        "--jars",
        findSparkConnectJar,
        "--conf",
        "spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin"))
    val process = if (isDebug) {
      builder.run(connectInput = true)
    } else {
      builder.run(ProcessLogger((_: String) => {}), connectInput = true)
    }

    // Adding JVM shutdown hook
    sys.addShutdownHook(kill())
    process
  }

  def start(): Unit = {
    assert(!stopped)
    sparkConnect
  }

  def kill(): Int = {
    stopped = true
    debug("Stopping the Spark Connect Server...")
    sparkConnect.destroy()
    val code = sparkConnect.exitValue()
    debug(s"Spark Connect Server is stopped with exit code: $code")
    code
  }

  private def findSparkConnectJar: String = {
    val target = "connector/connect/server/target"
    val parentDir = new File(sparkHome, target)
    assert(
      parentDir.exists(),
      s"Fail to locate the spark connect target folder: '${parentDir.getCanonicalPath}'. " +
        s"SPARK_HOME='${new File(sparkHome).getCanonicalPath}'. " +
        "Make sure env variable `SPARK_HOME` is set correctly.")
    val jars = recursiveListFiles(parentDir).filter { f =>
      // SBT jar
      (f.getParent.endsWith("scala-2.12") &&
        f.getName.startsWith("spark-connect-assembly") && f.getName.endsWith("SNAPSHOT.jar")) ||
      // Maven Jar
      (f.getParent.endsWith("target") &&
        f.getName.startsWith("spark-connect") && f.getName.endsWith("SNAPSHOT.jar"))
    }
    // It is possible we found more than one: one built by maven, and another by SBT
    assert(
      jars.nonEmpty,
      s"Failed to find the `spark-connect` jar inside folder: ${parentDir.getCanonicalPath}")
    jars(0).getCanonicalPath // return the first one
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
}

trait RemoteSparkSession extends AnyFunSuite with BeforeAndAfterAll { // scalastyle:ignore funsuite
  import SparkConnectServerUtils._
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkConnectServerUtils.start()
    spark = SparkSession.builder().build()

    // Retry and wait for the server to start
    val stop = System.currentTimeMillis() + 60000 * 1 // ~1 min
    var sleepInternal = 1000 // 1s with * 2 backoff
    var success = false
    val error = new RuntimeException(s"Failed to start the test server on port $port")

    while (!success && System.currentTimeMillis() < stop) {
      try {
        // Run a simple query to verify the server is really up and ready
        val result = spark
          .sql("select val from (values ('Hello'), ('World')) as t(val)")
          .collectResult()
          .toArray
        assert(result.length == 2)
        success = true
        debug("Spark Connect Server is up.")
      } catch {
        // ignored the error
        case e: Throwable =>
          error.addSuppressed(e)
          Thread.sleep(sleepInternal)
          sleepInternal *= 2
      }
    }

    // Throw error if failed
    if (!success) {
      throw error
    }
  }

  override def afterAll(): Unit = {
    try {
      spark.close()
    } catch {
      case e: Throwable => debug(e)
    }
    spark = null
    super.afterAll()
  }
}
