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

import org.apache.spark.util.Utils
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.SparkILoop

object Main extends Logging {

  val conf = new SparkConf()
  val tmp = System.getProperty("java.io.tmpdir")
  val rootDir = conf.get("spark.repl.classdir", tmp)
  val outputDir = Utils.createTempDir(rootDir)
  val s = new Settings()
  s.processArguments(List("-Yrepl-class-based",
    "-Yrepl-outdir", s"${outputDir.getAbsolutePath}", "-Yrepl-sync"), true)
  val classServer = new HttpServer(conf, outputDir, new SecurityManager(conf))
  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _
  var interp = new SparkILoop // this is a public var because tests reset it.

  def main(args: Array[String]) {
    if (getMaster == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")
    // Start the classServer and store its URI in a spark system property
    // (which will be passed to executors so that they can connect to it)
    classServer.start()
    interp.process(s) // Repl starts and goes in loop of R.E.P.L
    classServer.stop()
    Option(sparkContext).map(_.stop)
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
      .setAppName("Spark shell")
      .setJars(jars)
      .set("spark.repl.class.uri", classServer.uri)
    logInfo("Spark class server started at " + classServer.uri)
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
    }
    catch {
      case cnf: java.lang.ClassNotFoundException =>
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
