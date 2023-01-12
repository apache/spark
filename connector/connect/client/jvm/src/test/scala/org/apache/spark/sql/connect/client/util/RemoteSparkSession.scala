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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import scala.sys.process._

/**
 * Util classes to start a local spark connect server in different process for local E2E tests.
 * Command: bin/spark-shell \
 * --jars `ls connector/connect/server/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
 * --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
 */
object SparkConnectServerUtils {
  lazy val sparkConnect: Process =
    Process(
      "bin/spark-shell",
      Seq(
        "--jars",
        findSparkConnectJar,
        "--conf",
        "spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin")
    ).run

  def start(): String = {
    sparkConnect
    "started"
  }

  def kill(): String = {
    sparkConnect.destroy()
    "Killed"
  }

  def isAlive(): Boolean = {
    sparkConnect.isAlive()
  }

  private def findSparkConnectJar: String = {
    val sparkHome = System.getProperty("SPARK_HOME", "./")
    val target = "connector/connect/server/target"
    val parentDir = new File(sparkHome, target)
    val jars = recursiveListFiles(parentDir).filter { f =>
      // SBT jar
      (f.getParent.endsWith("scala-2.12") &&
        f.getName.startsWith("spark-connect-assembly") && f.getName.endsWith("SNAPSHOT.jar")) ||
        // Maven Jar
        (f.getParent.endsWith("target") &&
          f.getName.startsWith("spark-connect") && f.getName.endsWith("SNAPSHOT.jar"))
    }
    // It is possible we found more than one: one jar built by maven, and another by SBT
    assert(jars.nonEmpty,
      s"Failed to find the `spark-connect` jar inside folder: ${parentDir.getAbsolutePath}")
    jars(0).getAbsolutePath // return the first one
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
}

trait RemoteSparkSession extends AnyFunSuite with BeforeAndAfterAll { // scalastyle:ignore funsuite
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkConnectServerUtils.start()
    spark = SparkSession.builder().build()

/*    // Retry to wait for the server to start
    val stop = System.currentTimeMillis() + 60000 * 1 // 1 min
    var success = false
    while (!success && System.currentTimeMillis() < stop) {
      try {
        spark = SparkSession.builder().build()
        success = true
      } catch {
        // ignored the error
        case e: Throwable => Thread.sleep(1000)
      }
    }
    // Final try and throw error if failed
    if (!success) {
      spark = SparkSession.builder().build()
    }*/
  }

  // Move the stop logic to JVM shutdown
  override def afterAll(): Unit = {
    spark = null
    SparkConnectServerUtils.kill()
    super.afterAll()
  }
}
