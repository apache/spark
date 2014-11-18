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
import sbt.Classpaths.publishTask
import sbt.Keys._
import sbtunidoc.Plugin.genjavadocSettings
import sbtunidoc.Plugin.UnidocKeys.unidocGenjavadocVersion
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}
import net.virtualvoid.sbt.graph.Plugin.graphSettings

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val allProjects@Seq(bagel, catalyst, core, graphx, hive, hiveThriftServer, mllib, repl,
    sql, networkCommon, networkShuffle, streaming, streamingFlumeSink, streamingFlume, streamingKafka,
    streamingMqtt, streamingTwitter, streamingZeromq) =
    Seq("bagel", "catalyst", "core", "graphx", "hive", "hive-thriftserver", "mllib", "repl",
      "sql", "network-common", "network-shuffle", "streaming", "streaming-flume-sink",
      "streaming-flume", "streaming-kafka", "streaming-mqtt", "streaming-twitter",
      "streaming-zeromq").map(ProjectRef(buildLocation, _))

  val optionallyEnabledProjects@Seq(yarn, yarnStable, yarnAlpha, java8Tests,
    sparkGangliaLgpl, sparkKinesisAsl) = Seq("yarn", "yarn-stable", "yarn-alpha",
    "java8-tests", "ganglia-lgpl", "kinesis-asl").map(ProjectRef(buildLocation, _))

  val assemblyProjects@Seq(assembly, examples, networkYarn) =
    Seq("assembly", "examples", "network-yarn").map(ProjectRef(buildLocation, _))

  val tools = ProjectRef(buildLocation, "tools")
  // Root project.
  val spark = ProjectRef(buildLocation, "spark")
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
    var profiles: mutable.Seq[String] = mutable.Seq("sbt")
    if (Properties.envOrNone("SPARK_GANGLIA_LGPL").isDefined) {
      println("NOTE: SPARK_GANGLIA_LGPL is deprecated, please use -Pspark-ganglia-lgpl flag.")
      profiles ++= Seq("spark-ganglia-lgpl")
    }
    if (Properties.envOrNone("SPARK_HIVE").isDefined) {
      println("NOTE: SPARK_HIVE is deprecated, please use -Phive and -Phive-thriftserver flags.")
      profiles ++= Seq("hive", "hive-thriftserver")
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

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES") match {
    case None => backwardCompatibility
    case Some(v) =>
      if (backwardCompatibility.nonEmpty)
        println("Note: We ignore environment variables, when use of profile is detected in " +
          "conjunction with environment variable.")
      v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }

    if (profiles.exists(_.contains("scala-"))) {
      profiles
    } else if (System.getProperty("scala-2.11") != null) {
      profiles ++ Seq("scala-2.11")
    } else {
      println("Enabled default scala profile")
      profiles ++ Seq("scala-2.10")
    }
  }

  Properties.envOrNone("SBT_MAVEN_PROPERTIES") match {
    case Some(v) =>
      v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.split("=")).foreach(x => System.setProperty(x(0), x(1)))
    case _ =>
  }

  override val userPropertiesMap = System.getProperties.toMap

  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  lazy val sharedSettings = graphSettings ++ genjavadocSettings ++ Seq (
    javaHome   := Properties.envOrNone("JAVA_HOME").map(file),
    incOptions := incOptions.value.withNameHashing(true),
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    publishMavenStyle := true,
    unidocGenjavadocVersion := "0.8",

    resolvers += Resolver.mavenLocal,
    otherResolvers <<= SbtPomKeys.mvnLocalRepository(dotM2 => Seq(Resolver.file("dotM2", dotM2))),
    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ optionallyEnabledProjects ++ assemblyProjects ++ Seq(spark, tools))
    .foreach(enable(sharedSettings ++ ExludedDependencies.settings))

  /* Enable tests settings for all projects except examples, assembly and tools */
  (allProjects ++ optionallyEnabledProjects).foreach(enable(TestSettings.settings))

  // TODO: Add Sql to mima checks
  allProjects.filterNot(x => Seq(spark, sql, hive, hiveThriftServer, catalyst, repl,
    streamingFlumeSink, networkCommon, networkShuffle, networkYarn).contains(x)).foreach {
      x => enable(MimaBuild.mimaSettings(sparkHome, x))(x)
    }

  /* Enable Assembly for all assembly projects */
  assemblyProjects.foreach(enable(Assembly.settings))

  /* Enable unidoc only for the root spark project */
  enable(Unidoc.settings)(spark)

  /* Catalyst macro settings */
  enable(Catalyst.settings)(catalyst)

  /* Spark SQL Core console settings */
  enable(SQL.settings)(sql)

  /* Hive console settings */
  enable(Hive.settings)(hive)

  enable(Flume.settings)(streamingFlumeSink)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    } ++ Seq[Project](OldDeps.project)
  }

}

object Flume {
  lazy val settings = sbtavro.SbtAvro.avroSettings
}

/**
  This excludes library dependencies in sbt, which are specified in maven but are
  not needed by sbt build.
  */
object ExludedDependencies {
  lazy val settings = Seq(
    libraryDependencies ~= { libs => libs.filterNot(_.name == "groovy-all") }
  )
}

/**
 * Following project only exists to pull previous artifacts of Spark for generating
 * Mima ignores. For more information see: SPARK 2071
 */
object OldDeps {

  lazy val project = Project("oldDeps", file("dev"), settings = oldDepsSettings)

  def versionArtifact(id: String): Option[sbt.ModuleID] = {
    val fullId = id + "_2.10"
    Some("org.apache.spark" % fullId % "1.1.0")
  }

  def oldDepsSettings() = Defaults.coreDefaultSettings ++ Seq(
    name := "old-deps",
    scalaVersion := "2.10.4",
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    libraryDependencies := Seq("spark-streaming-mqtt", "spark-streaming-zeromq",
      "spark-streaming-flume", "spark-streaming-kafka", "spark-streaming-twitter",
      "spark-streaming", "spark-mllib", "spark-bagel", "spark-graphx",
      "spark-core").map(versionArtifact(_).get intransitive())
  )
}

object Catalyst {
  lazy val settings = Seq(
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full),
    // Quasiquotes break compiling scala doc...
    // TODO: Investigate fixing this.
    sources in (Compile, doc) ~= (_ filter (_.getName contains "codegen")))
}

object SQL {
  lazy val settings = Seq(
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
        |import org.apache.spark.sql.test.TestSQLContext._
        |import org.apache.spark.sql.parquet.ParquetTestData""".stripMargin,
    cleanupCommands in console := "sparkContext.stop()"
  )
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
        |import org.apache.spark.sql.parquet.ParquetTestData""".stripMargin,
    cleanupCommands in console := "sparkContext.stop()",
    // Some of our log4j jars make it impossible to submit jobs from this JVM to Hive Map/Reduce
    // in order to generate golden files.  This is only required for developers who are adding new
    // new query tests.
    fullClasspath in Test := (fullClasspath in Test).value.filterNot { f => f.toString.contains("jcl-over") }
  )

}

object Assembly {
  import sbtassembly.Plugin._
  import AssemblyKeys._

  lazy val settings = assemblySettings ++ Seq(
    test in assembly := {},
    jarName in assembly <<= (version, moduleName) map { (v, mName) =>
      if (mName.contains("network-yarn")) {
        // This must match the same name used in maven (see network/yarn/pom.xml)
        "spark-" + v + "-yarn-shuffle.jar"
      } else {
        mName + "-" + v + "-hadoop" +
          Option(System.getProperty("hadoop.version")).getOrElse("1.0.4") + ".jar"
      }
    },
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
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, catalyst, streamingFlumeSink, yarn, yarnAlpha),
    unidocProjectFilter in(JavaUnidoc, unidoc) :=
      inAnyProject -- inProjects(OldDeps.project, repl, bagel, graphx, examples, tools, catalyst, streamingFlumeSink, yarn, yarnAlpha),

    // Skip class names containing $ and some internal packages in Javadocs
    unidocAllSources in (JavaUnidoc, unidoc) := {
      (unidocAllSources in (JavaUnidoc, unidoc)).value
        .map(_.filterNot(_.getName.contains("$")))
        .map(_.filterNot(_.getCanonicalPath.contains("akka")))
        .map(_.filterNot(_.getCanonicalPath.contains("deploy")))
        .map(_.filterNot(_.getCanonicalPath.contains("network")))
        .map(_.filterNot(_.getCanonicalPath.contains("shuffle")))
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
        "streaming.mqtt", "streaming.twitter", "streaming.zeromq", "streaming.kinesis"
      ),
      "-group", "MLlib", packageList(
        "mllib.classification", "mllib.clustering", "mllib.evaluation.binary", "mllib.linalg",
        "mllib.linalg.distributed", "mllib.optimization", "mllib.rdd", "mllib.recommendation",
        "mllib.regression", "mllib.stat", "mllib.tree", "mllib.tree.configuration",
        "mllib.tree.impurity", "mllib.tree.model", "mllib.util"
      ),
      "-group", "Spark SQL", packageList("sql.api.java", "sql.api.java.types", "sql.hive.api.java"),
      "-noqualifier", "java.lang"
    )
  )
}

object TestSettings {
  import BuildCommons._

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    javaOptions in Test += "-Dspark.test.home=" + sparkHome,
    javaOptions in Test += "-Dspark.testing=1",
    javaOptions in Test += "-Dspark.port.maxRetries=100",
    javaOptions in Test += "-Dspark.ui.enabled=false",
    javaOptions in Test += "-Dspark.ui.showConsoleProgress=false",
    javaOptions in Test += "-Dspark.driver.allowMultipleContexts=true",
    javaOptions in Test += "-Dsun.io.serialization.extendedDebugInfo=true",
    javaOptions in Test ++= System.getProperties.filter(_._1 startsWith "spark")
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    javaOptions in Test ++= "-Xmx3g -XX:PermSize=128M -XX:MaxNewSize=256m -XX:MaxPermSize=1g"
      .split(" ").toSeq,
    // This places test scope jars on the classpath of executors during tests.
    javaOptions in Test += 
      "-Dspark.executor.extraClassPath=" + (fullClasspath in Test).value.files.
      map(_.getAbsolutePath).mkString(":").stripSuffix(":"),
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
