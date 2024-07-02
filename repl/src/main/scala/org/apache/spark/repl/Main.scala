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

package org.apache.spark.repl

import java.io.File
import java.net.URI

import scala.tools.nsc.GenericRunnerSettings

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils

object Main extends Logging {

  initializeLogIfNecessary(true)
  Signaling.cancelOnInterrupt()

  val conf = new SparkConf()
  val rootDir =
    conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
  val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: SparkILoop = _

  private var hasErrors = false
  private var isShellSession = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }

  def main(args: Array[String]): Unit = {
    isShellSession = true
    doMain(args, new SparkILoop)
  }

  // Visible for testing
  private[repl] def doMain(args: Array[String], _interp: SparkILoop): Unit = {
    interp = _interp
    val jars = Utils
      .getLocalUserJarsForShell(conf)
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x =>
        if (x.startsWith("file:")) new File(new URI(x)).getPath else x
      }
      .mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir",
      s"${outputDir.getAbsolutePath}",
      "-classpath",
      jars
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)

    if (!hasErrors) {
      interp.run(settings) // Repl starts and goes in loop of R.E.P.L
      Option(sparkContext).foreach(_.stop())
    }
  }

  def createSparkSession(): SparkSession = {
    try {
      val execUri = System.getenv("SPARK_EXECUTOR_URI")
      conf.setIfMissing("spark.app.name", "Spark shell")
      // SparkContext will detect this configuration and register it with the RpcEnv's
      // file server, setting spark.repl.class.uri to the actual URI for executors to
      // use. This is sort of ugly but since executors are started as part of SparkContext
      // initialization in certain cases, there's an initialization order issue that prevents
      // this from being set after SparkContext is instantiated.
      conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
      if (execUri != null) {
        conf.set("spark.executor.uri", execUri)
      }
      if (System.getenv("SPARK_HOME") != null) {
        conf.setSparkHome(System.getenv("SPARK_HOME"))
      }

      val builder = SparkSession.builder().config(conf)
      if (conf.get(CATALOG_IMPLEMENTATION.key, "hive") == "hive") {
        if (SparkSession.hiveClassesArePresent) {
          // In the case that the property is not set at all, builder's config
          // does not have this value set to 'hive' yet. The original default
          // behavior is that when there are hive classes, we use hive catalog.
          sparkSession = builder.enableHiveSupport().getOrCreate()
          logInfo("Created Spark session with Hive support")
        } else {
          // Need to change it back to 'in-memory' if no hive classes are found
          // in the case that the property is set to hive in spark-defaults.conf
          builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
          sparkSession = builder.getOrCreate()
          logInfo("Created Spark session")
        }
      } else {
        // In the case that the property is set but not to 'hive', the internal
        // default is 'in-memory'. So the sparkSession will use in-memory catalog.
        sparkSession = builder.getOrCreate()
        logInfo("Created Spark session")
      }
      sparkContext = sparkSession.sparkContext
      sparkSession
    } catch {
      case e: Exception if isShellSession =>
        logError("Failed to initialize Spark session.", e)
        sys.exit(1)
    }
  }

}
