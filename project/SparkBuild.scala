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
  
  val sparkVersion = "1.0.0-SNAPSHOT"

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val allProjects@Seq(bagel, catalyst, core, graphx, hive, mllib, repl, spark, sql, streaming,
  streamingFlume, streamingKafka, streamingMqtt, streamingTwitter, streamingZeromq) =
    Seq("bagel", "catalyst", "core", "graphx", "hive", "mllib", "repl", "spark", "sql",
      "streaming", "streaming-flume", "streaming-kafka", "streaming-mqtt", "streaming-twitter",
      "streaming-zeromq").map(ProjectRef(buildLocation, _))

  val optionallyEnabledProjects@Seq(yarn, yarnStable, yarnAlpha, java8Tests, sparkGangliaLgpl) =
    Seq("yarn", "yarn-stable", "yarn-alpha", "java8-tests", "ganglia-lgpl").map(ProjectRef(buildLocation, _))

  val assemblyProjects@Seq(assembly, examples, tools) = Seq("assembly", "examples", "tools")
    .map(ProjectRef(buildLocation, _))

  val sparkHome = buildLocation
}

object SparkBuild extends PomBuild {

  import BuildCommons._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  def backwardCompatibility = {
    import scala.collection.mutable
    var profiles: mutable.Seq[String] = mutable.Seq.empty
    if (Properties.envOrNone("SPARK_YARN").isDefined) profiles ++= Seq("yarn")
    if (Properties.envOrNone("SPARK_GANGLIA_LGPL").isDefined) profiles ++= Seq("spark-ganglia-lgpl")
    if (Properties.envOrNone("SPARK_HIVE").isDefined) profiles ++= Seq("hive")
    Properties.envOrNone("SPARK_HADOOP_VERSION") match {
      case Some(v) => System.setProperty("hadoop.version", v)
      case None =>
    }
    profiles
  }

  override val profiles = Properties.envOrNone("MAVEN_PROFILES") match {
    case None => backwardCompatibility
    // Rationale: If -P option exists no need to support backwardCompatibility.
    case Some(v) => v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
  }

  override val userPropertiesMap = System.getProperties.toMap

  lazy val sharedSettings = graphSettings ++ ScalaStyleSettings ++ Seq (
    javaHome   := Properties.envOrNone("JAVA_HOME").map(file),
    incOptions := incOptions.value.withNameHashing(true),
    publishMavenStyle := true
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  allProjects ++ optionallyEnabledProjects ++ assemblyProjects foreach enable(sharedSettings)

  /* Enable tests settings for all projects except examples, assembly and tools */
  allProjects ++ optionallyEnabledProjects foreach enable(TestSettings.s)

  /* Enable Mima for all projects except spark, sql, hive, catalyst  and repl */
  allProjects filterNot(y => Seq(spark, sql, hive, catalyst, repl).exists(x => x == y)) foreach (x => enable(MimaSettings.effectiveSetting(x))(x))

  /* Enable Assembly for all assembly projects */
  assemblyProjects foreach enable(AssemblySettings.s)

  /* Enable unidoc only for the root spark project */
  Seq(spark) foreach enable (UnidocSettings.s)

  /* Hive console settings */
  Seq(hive) foreach enable (hiveSettings)

  lazy val hiveSettings = Seq(

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

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    }
  }

}

object MimaSettings {

  import BuildCommons._
  import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

  private lazy val s = MimaBuild.mimaSettings(sparkHome)

  def effectiveSetting(projectRef: ProjectRef) = {
    val organization = "org.apache.spark"
    val version = "0.9.0-incubating"
    val fullId = "spark-" + projectRef.project + "_2.10"
    s ++ Seq(previousArtifact := Some(organization % fullId % version))
  }
}

object AssemblySettings {
  import sbtassembly.Plugin._
  import AssemblyKeys._

  lazy val s = assemblySettings ++ Seq(
    test in assembly := {},
    jarName in assembly <<= (version, moduleName) map { (v, mName) => mName + "-"+v + "-hadoop" +
      Option(System.getProperty("hadoop.version")).getOrElse("1.0.4") + ".jar" }, // TODO: add proper default hadoop version.
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

object UnidocSettings {

  import BuildCommons._
  import sbtunidoc.Plugin._
  import UnidocKeys._

  // for easier specification of JavaDoc package groups
  private def packageList(names: String*): String = {
    names.map(s => "org.apache.spark." + s).mkString(":")
  }

  lazy val s = scalaJavaUnidocSettings ++ Seq (
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
      "-doc-title", "Spark " + sparkVersion.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    ),

    // Javadoc options: create a window title, and group key packages on index page
    javacOptions in doc := Seq(
      "-windowtitle", "Spark " + sparkVersion.replaceAll("-SNAPSHOT", "") + " JavaDoc",
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

  lazy val s = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    javaOptions in Test += "-Dspark.home=" + sparkHome,
    javaOptions in Test += "-Dspark.testing=1",
    javaOptions in Test += "-Dsun.io.serialization.extendedDebugInfo=true",
    javaOptions in Test ++= System.getProperties.filter(_._1 startsWith "spark").map { case (k,v) => s"-D$k=$v" }.toSeq,
    javaOptions in Test ++= "-Xmx3g -XX:PermSize=128M -XX:MaxNewSize=256m -XX:MaxPermSize=1g".split(" ").toSeq,
    javaOptions += "-Xmx3g",

    // Show full stack trace and duration in test cases.
    testOptions in Test += Tests.Argument("-oDF"),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test",
    // Only allow one test at a time, even across projects, since they run in the same JVM
    parallelExecution in Test := false,
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1))

}
