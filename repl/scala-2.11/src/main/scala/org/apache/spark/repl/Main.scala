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

import scala.tools.nsc.GenericRunnerSettings

import org.apache.spark.util.Utils
import org.apache.spark._
import org.apache.spark.sql.SQLContext

object Main extends Logging {

  val conf = new SparkConf()
  val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
  val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _
  // this is a public var because tests reset it.
  var interp: SparkILoop = _

  private var hasErrors = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    Console.err.println(msg)
  }

  def main(args: Array[String]) {
    doMain(args, new SparkILoop)
  }

  // Visible for testing
  private[repl] def doMain(args: Array[String], _interp: SparkILoop): Unit = {
    interp = _interp
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", getAddedJars.mkString(File.pathSeparator)
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)

    if (!hasErrors) {
      if (getMaster == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")
      interp.process(settings) // Repl starts and goes in loop of R.E.P.L
      Option(sparkContext).map(_.stop)
    }
  }

  def getAddedJars: Array[String] = {
    val envJars = sys.env.get("ADD_JARS")
    if (envJars.isDefined) {
      logWarning("ADD_JARS environment variable is deprecated, use --jar spark submit argument instead")
    }
    val propJars = sys.props.get("spark.jars").flatMap { p => if (p == "") None else Some(p) }
    val jars = propJars.orElse(envJars).getOrElse("")
    Utils.resolveURIs(jars).split(",").filter(_.nonEmpty)
  }

  def createSparkContext(): SparkContext = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val jars = getAddedJars
    val conf = new SparkConf()
      .setMaster(getMaster)
      .setJars(jars)
      .setIfMissing("spark.app.name", "Spark shell")
      // SparkContext will detect this configuration and register it with the RpcEnv's
      // file server, setting spark.repl.class.uri to the actual URI for executors to
      // use. This is sort of ugly but since executors are started as part of SparkContext
      // initialization in certain cases, there's an initialization order issue that prevents
      // this from being set after SparkContext is instantiated.
      .set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }
    sparkContext = new SparkContext(conf)
    logInfo("Created spark context..")
    sparkContext
  }

  def createSQLContext(): SQLContext = {
    val name = "org.apache.spark.sql.hive.HiveContext"
    val loader = Utils.getContextOrSparkClassLoader
    try {
      sqlContext = loader.loadClass(name).getConstructor(classOf[SparkContext])
        .newInstance(sparkContext).asInstanceOf[SQLContext]
      logInfo("Created sql context (with Hive support)..")
    } catch {
      case _: java.lang.ClassNotFoundException | _: java.lang.NoClassDefFoundError =>
        sqlContext = new SQLContext(sparkContext)
        logInfo("Created sql context..")
    }
    sqlContext
  }

  private def getMaster: String = {
    val master = {
      val envMaster = sys.env.get("MASTER")
      val propMaster = sys.props.get("spark.master")
      propMaster.orElse(envMaster).getOrElse("local[*]")
    }
    master
  }
}
