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

import scala.util.Properties
import scala.collection.JavaConversions._

import sbt._
import sbt.Keys._
import org.scalastyle.sbt.ScalastylePlugin.{Settings => ScalaStyleSettings}
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}
import net.virtualvoid.sbt.graph.Plugin.graphSettings

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val allProjects@Seq(bagel, catalyst, core, graphx, hive, mllib, repl, spark, sql, streaming,
  streamingFlume, streamingKafka, streamingMqtt, streamingTwitter, streamingZeromq) =
    Seq("bagel", "catalyst", "core", "graphx", "hive", "mllib", "repl", "spark", "sql",
      "streaming", "streaming-flume", "streaming-kafka", "streaming-mqtt", "streaming-twitter",
      "streaming-zeromq").map(ProjectRef(buildLocation, _))

  val optionallyEnabledProjects@Seq(yarn, yarnStable, yarnAlpha, java8Tests, sparkGangliaLgpl) =
    Seq("yarn", "yarn-stable", "yarn-alpha", "java8-tests", "ganglia-lgpl")
      .map(ProjectRef(buildLocation, _))

  val assemblyProjects@Seq(assembly, examples) = Seq("assembly", "examples")
    .map(ProjectRef(buildLocation, _))

  val tools = "tools"

  val sparkHome = buildLocation
}

object SparkBuild extends PomBuild {

  import BuildCommons._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  // Provides compatibility for older versions of the Spark build
  def backwardCompatibility = {
    import scala.collection.mutable
    var isAlphaYarn = false
    var profiles: mutable.Seq[String] = mutable.Seq.empty
    if (Properties.envOrNone("SPARK_GANGLIA_LGPL").isDefined) {
      println("NOTE: SPARK_GANGLIA_LGPL is deprecated, please use -Pganglia-lgpl flag.")
      profiles ++= Seq("spark-ganglia-lgpl")
    }
    if (Properties.envOrNone("SPARK_HIVE").isDefined) {
      println("NOTE: SPARK_HIVE is deprecated, please use -Phive flag.")
      profiles ++= Seq("hive")
    }
    Properties.envOrNone("SPARK_HADOOP_VERSION") match {
      case Some(v) =>
        if (v.matches("0.23.*")) isAlphaYarn = true
        println("NOTE: SPARK_HADOOP_VERSION is deprecated, please use -Dhadoop.version=" + v)
        System.setProperty("hadoop.version", v)
      case None =>
    }
    if (Properties.envOrNone("SPARK_YARN").isDefined) {
      if(isAlphaYarn) {
        println("NOTE: SPARK_YARN is deprecated, please use -Pyarn-alpha flag.")
        profiles ++= Seq("yarn-alpha")
      }
      else {
        println("NOTE: SPARK_YARN is deprecated, please use -Pyarn flag.")
        profiles ++= Seq("yarn")
      }
    }
    profiles
  }

  override val profiles = Properties.envOrNone("MAVEN_PROFILES") match {
    case None => backwardCompatibility
    // Rationale: If -P option exists no need to support backwardCompatibility.
    case Some(v) =>
      if (backwardCompatibility.nonEmpty)
        println("Note: We ignore environment variables, when use of profile is detected in " +
          "conjunction with environment variable.")
      v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
  }

  override val userPropertiesMap = System.getProperties.toMap

  lazy val sharedSettings = graphSettings ++ ScalaStyleSettings ++ Seq (
    javaHome   := Properties.envOrNone("JAVA_HOME").map(file),
    incOptions := incOptions.value.withNameHashing(true),
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    publishMavenStyle := true
  )

  /** Following project only exists to pull previous artifacts of Spark for generating
    Mima ignores. For more information see: SPARK 2071 */
  lazy val oldDeps = Project("oldDeps", file("dev"), settings = oldDepsSettings)

  def versionArtifact(id: String): Option[sbt.ModuleID] = {
    val fullId = id + "_2.10"
    Some("org.apache.spark" % fullId % "1.0.0")
  }

  def oldDepsSettings() = Defaults.defaultSettings ++ Seq(
    name := "old-deps",
    scalaVersion := "2.10.4",
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    libraryDependencies := Seq("spark-streaming-mqtt", "spark-streaming-zeromq",
      "spark-streaming-flume", "spark-streaming-kafka", "spark-streaming-twitter",
      "spark-streaming", "spark-mllib", "spark-bagel", "spark-graphx",
      "spark-core").map(versionArtifact(_).get intransitive())
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ optionallyEnabledProjects ++ assemblyProjects).foreach(enable(sharedSettings))

  /* Enable tests settings for all projects except examples, assembly and tools */
  (allProjects ++ optionallyEnabledProjects).foreach(enable(TestSettings.settings))

  /* Enable Mima for all projects except spark, hive, catalyst, sql  and repl */
  // TODO: Add Sql to mima checks
  allProjects.filterNot(y => Seq(spark, sql, hive, catalyst, repl).exists(x => x == y)).
    foreach (x => enable(MimaBuild.mimaSettings(sparkHome, x))(x))

  /* Enable Assembly for all assembly projects */
  assemblyProjects.foreach(enable(Assembly.settings))

  /* Enable unidoc only for the root spark project */
  enable(Unidoc.settings)(spark)

  /* Hive console settings */
  enable(Hive.settings)(hive)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    } ++ Seq[Project](oldDeps)
  }

}

object Hive {

  lazy val settings = Seq(

    javaOptions += "-XX:MaxPermSize=1g",
    // Multiple queries rely on the TestHive singleton. See comments there for more details.
    parallelExecution in Test := false,
    // Supporting all SerDes requires us to depend on deprecated APIs, so we turn off the warnings
    // only for this subproject.
    scalacOptions <<= scalacOptions map { currentOpts: Seq[String] =>
      currentOpts.filterNot(_ == "-deprecation")
    },
    initialCommands in console :=
      """
        |import org.apache.spark.sql.catalyst.analysis._
        |import org.apache.spark.sql.catalyst.dsl._
        |import org.apache.spark.sql.catalyst.errors._
        |import org.apache.spark.sql.catalyst.expressions._
        |import org.apache.spark.sql.catalyst.plans.logical._
        |import org.apache.spark.sql.catalyst.rules._
        |import org.apache.spark.sql.catalyst.types._
        |import org.apache.spark.sql.catalyst.util._
        |import org.apache.spark.sql.execution
        |import org.apache.spark.sql.hive._
        |import org.apache.spark.sql.hive.test.TestHive._
        |import org.apache.spark.sql.parquet.ParquetTestData""".stripMargin
  )

}

object Assembly {
  import sbtassembly.Plugin._
  import AssemblyKeys._

  lazy val settings = assemblySettings ++ Seq(
    test in assembly := {},
    jarName in assembly <<= (version, moduleName) map { (v, mName) => mName + "-"+v + "-hadoop" +
      Option(System.getProperty("hadoop.version")).getOrElse("1.0.4") + ".jar" },
    mergeStrategy in assembly := {
      case PathList("org", "datanucleus", xs @ _*)             => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
      case "log4j.properties"                                  => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )

}

object Unidoc {

  import BuildCommons._
  import sbtunidoc.Plugin._
  import UnidocKeys._

  // for easier specification of JavaDoc package groups
  private def packageList(names: String*): String = {
    names.map(s => "org.apache.spark." + s).mkString(":")
  }

  lazy val settings = scalaJavaUnidocSettings ++ Seq (
    publish := {},

    unidocProjectFilter in(ScalaUnidoc, unidoc) :=
      inAnyProject -- inProjects(repl, examples, tools, catalyst, yarn, yarnAlpha),
    unidocProjectFilter in(JavaUnidoc, unidoc) :=
      inAnyProject -- inProjects(repl, bagel, graphx, examples, tools, catalyst, yarn, yarnAlpha),

    // Skip class names containing $ and some internal packages in Javadocs
    unidocAllSources in (JavaUnidoc, unidoc) := {
      (unidocAllSources in (JavaUnidoc, unidoc)).value
        .map(_.filterNot(_.getName.contains("$")))
        .map(_.filterNot(_.getCanonicalPath.contains("akka")))
        .map(_.filterNot(_.getCanonicalPath.contains("deploy")))
        .map(_.filterNot(_.getCanonicalPath.contains("network")))
        .map(_.filterNot(_.getCanonicalPath.contains("executor")))
        .map(_.filterNot(_.getCanonicalPath.contains("python")))
        .map(_.filterNot(_.getCanonicalPath.contains("collection")))
    },

    // Javadoc options: create a window title, and group key packages on index page
    javacOptions in doc := Seq(
      "-windowtitle", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-public",
      "-group", "Core Java API", packageList("api.java", "api.java.function"),
      "-group", "Spark Streaming", packageList(
        "streaming.api.java", "streaming.flume", "streaming.kafka",
        "streaming.mqtt", "streaming.twitter", "streaming.zeromq"
      ),
      "-group", "MLlib", packageList(
        "mllib.classification", "mllib.clustering", "mllib.evaluation.binary", "mllib.linalg",
        "mllib.linalg.distributed", "mllib.optimization", "mllib.rdd", "mllib.recommendation",
        "mllib.regression", "mllib.stat", "mllib.tree", "mllib.tree.configuration",
        "mllib.tree.impurity", "mllib.tree.model", "mllib.util"
      ),
      "-group", "Spark SQL", packageList("sql.api.java", "sql.hive.api.java"),
      "-noqualifier", "java.lang"
    )
  )
}

object TestSettings {
  import BuildCommons._

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    javaOptions in Test += "-Dspark.home=" + sparkHome,
    javaOptions in Test += "-Dspark.testing=1",
    javaOptions in Test += "-Dsun.io.serialization.extendedDebugInfo=true",
    javaOptions in Test ++= System.getProperties.filter(_._1 startsWith "spark")
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    javaOptions in Test ++= "-Xmx3g -XX:PermSize=128M -XX:MaxNewSize=256m -XX:MaxPermSize=1g"
      .split(" ").toSeq,
    javaOptions += "-Xmx3g",

    // Show full stack trace and duration in test cases.
    testOptions in Test += Tests.Argument("-oDF"),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test",
    // Only allow one test at a time, even across projects, since they run in the same JVM
    parallelExecution in Test := false,
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
    // Remove certain packages from Scaladoc
    scalacOptions in (Compile, doc) := Seq(
      "-groups",
      "-skip-packages", Seq(
        "akka",
        "org.apache.spark.api.python",
        "org.apache.spark.network",
        "org.apache.spark.deploy",
        "org.apache.spark.util.collection"
      ).mkString(":"),
      "-doc-title", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    )
  )

}
