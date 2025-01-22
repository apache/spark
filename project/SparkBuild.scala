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

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Locale

import scala.io.Source
import scala.util.Properties
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import sbt._
import sbt.Classpaths.publishOrSkip
import sbt.Keys._
import sbt.librarymanagement.{ VersionNumber, SemanticSelector }
import com.etsy.sbt.checkstyle.CheckstylePlugin.autoImport._
import com.simplytyped.Antlr4Plugin._
import sbtpomreader.{PomBuild, SbtPomKeys}
import com.typesafe.tools.mima.plugin.MimaKeys
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks
import sbtassembly.AssemblyPlugin.autoImport._

import spray.revolver.RevolverPlugin._

import sbtprotoc.ProtocPlugin.autoImport._

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val sqlProjects@Seq(sqlApi, catalyst, sql, hive, hiveThriftServer, tokenProviderKafka010, sqlKafka010, avro, protobuf) =
    Seq("sql-api", "catalyst", "sql", "hive", "hive-thriftserver", "token-provider-kafka-0-10",
      "sql-kafka-0-10", "avro", "protobuf").map(ProjectRef(buildLocation, _))

  val streamingProjects@Seq(streaming, streamingKafka010) =
    Seq("streaming", "streaming-kafka-0-10").map(ProjectRef(buildLocation, _))

  val connectProjects@Seq(connectCommon, connect, connectClient, connectShims) =
    Seq("connect-common", "connect", "connect-client-jvm", "connect-shims")
      .map(ProjectRef(buildLocation, _))

  val allProjects@Seq(
    core, graphx, mllib, mllibLocal, repl, networkCommon, networkShuffle, launcher, unsafe, tags, sketch, kvstore,
    commonUtils, variant, _*
  ) = Seq(
    "core", "graphx", "mllib", "mllib-local", "repl", "network-common", "network-shuffle", "launcher", "unsafe",
    "tags", "sketch", "kvstore", "common-utils", "variant"
  ).map(ProjectRef(buildLocation, _)) ++ sqlProjects ++ streamingProjects ++ connectProjects

  val optionallyEnabledProjects@Seq(kubernetes, yarn,
    sparkGangliaLgpl, streamingKinesisAsl, profiler,
    dockerIntegrationTests, hadoopCloud, kubernetesIntegrationTests) =
    Seq("kubernetes", "yarn",
      "ganglia-lgpl", "streaming-kinesis-asl", "profiler",
      "docker-integration-tests", "hadoop-cloud", "kubernetes-integration-tests").map(ProjectRef(buildLocation, _))

  val assemblyProjects@Seq(networkYarn, streamingKafka010Assembly, streamingKinesisAslAssembly) =
    Seq("network-yarn", "streaming-kafka-0-10-assembly", "streaming-kinesis-asl-assembly")
      .map(ProjectRef(buildLocation, _))

  val copyJarsProjects@Seq(assembly, examples) = Seq("assembly", "examples")
    .map(ProjectRef(buildLocation, _))

  val tools = ProjectRef(buildLocation, "tools")
  // Root project.
  val spark = ProjectRef(buildLocation, "spark")
  val sparkHome = buildLocation

  val testTempDir = s"$sparkHome/target/tmp"

  val javaVersion = settingKey[String]("source and target JVM version for javac and scalac")

  // Google Protobuf version used for generating the protobuf.
  // SPARK-41247: needs to be consistent with `protobuf.version` in `pom.xml`.
  val protoVersion = "4.29.3"
}

object SparkBuild extends PomBuild {

  import BuildCommons._
  import sbtunidoc.GenJavadocPlugin
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
      case None => Seq("sbt")
      case Some(v) =>
        v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }
    if (profiles.contains("jdwp-test-debug")) {
      sys.props.put("test.jdwp.enabled", "true")
    }
    if (profiles.contains("user-defined-protoc")) {
      val sparkProtocExecPath = Properties.envOrNone("SPARK_PROTOC_EXEC_PATH")
      val connectPluginExecPath = Properties.envOrNone("CONNECT_PLUGIN_EXEC_PATH")
      if (sparkProtocExecPath.isDefined) {
        sys.props.put("spark.protoc.executable.path", sparkProtocExecPath.get)
      }
      if (connectPluginExecPath.isDefined) {
        sys.props.put("connect.plugin.executable.path", connectPluginExecPath.get)
      }
    }
    profiles
  }

  Properties.envOrNone("SBT_MAVEN_PROPERTIES") match {
    case Some(v) =>
      v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.split("=")).foreach(x => System.setProperty(x(0), x(1)))
    case _ =>
  }

  override val userPropertiesMap = System.getProperties.asScala.toMap

  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val SbtCompile = config("sbt") extend(Compile)

  lazy val sparkGenjavadocSettings: Seq[sbt.Def.Setting[_]] = GenJavadocPlugin.projectSettings ++ Seq(
    scalacOptions ++= Seq(
      "-P:genjavadoc:strictVisibility=true" // hide package private types
    )
  )

  lazy val scalaStyleRules = Project("scalaStyleRules", file("scalastyle"))
    .settings(
      libraryDependencies += "org.scalastyle" %% "scalastyle" % "1.0.0"
    )

  lazy val scalaStyleOnCompile = taskKey[Unit]("scalaStyleOnCompile")

  lazy val scalaStyleOnTest = taskKey[Unit]("scalaStyleOnTest")

  // We special case the 'println' lint rule to only be a warning on compile, because adding
  // printlns for debugging is a common use case and is easy to remember to remove.
  val scalaStyleOnCompileConfig: String = {
    val in = "scalastyle-config.xml"
    val out = "scalastyle-on-compile.generated.xml"
    val replacements = Map(
      """customId="println" level="error"""" -> """customId="println" level="warn""""
    )
    val source = Source.fromFile(in)
    try {
      var contents = source.getLines.mkString("\n")
      for ((k, v) <- replacements) {
        require(contents.contains(k), s"Could not rewrite '$k' in original scalastyle config.")
        contents = contents.replace(k, v)
      }
      new PrintWriter(out) {
        write(contents)
        close()
      }
      out
    } finally {
      source.close()
    }
  }

  // Return a cached scalastyle task for a given configuration (usually Compile or Test)
  private def cachedScalaStyle(config: Configuration) = Def.task {
    val logger = streams.value.log
    // We need a different cache dir per Configuration, otherwise they collide
    val cacheDir = target.value / s"scalastyle-cache-${config.name}"
    val cachedFun = FileFunction.cached(cacheDir, FilesInfo.lastModified, FilesInfo.exists) {
      (inFiles: Set[File]) => {
        val args: Seq[String] = Seq.empty
        val scalaSourceV = Seq(file((config / scalaSource).value.getAbsolutePath))
        val configV = (ThisBuild / baseDirectory).value / scalaStyleOnCompileConfig
        val configUrlV = (config / scalastyleConfigUrl).value
        val streamsV = ((config / streams).value: @sbtUnchecked)
        val failOnErrorV = true
        val failOnWarningV = false
        val scalastyleTargetV = (config / scalastyleTarget).value
        val configRefreshHoursV = (config / scalastyleConfigRefreshHours).value
        val targetV = (config / target).value
        val configCacheFileV = (config / scalastyleConfigUrlCacheFile).value

        logger.info(s"Running scalastyle on ${name.value} in ${config.name}")
        Tasks.doScalastyle(args, configV, configUrlV, failOnErrorV, failOnWarningV, scalaSourceV,
          scalastyleTargetV, streamsV, configRefreshHoursV, targetV, configCacheFileV)

        Set.empty
      }
    }

    cachedFun(findFiles((config / scalaSource).value))
  }

  private def findFiles(file: File): Set[File] = if (file.isDirectory) {
    file.listFiles().toSet.flatMap(findFiles) + file
  } else {
    Set(file)
  }

  def enableScalaStyle: Seq[sbt.Def.Setting[_]] = Seq(
    scalaStyleOnCompile := cachedScalaStyle(Compile).value,
    scalaStyleOnTest := cachedScalaStyle(Test).value,
    (scalaStyleOnCompile / logLevel) := Level.Warn,
    (scalaStyleOnTest / logLevel) := Level.Warn,
    (Compile / compile) := {
      scalaStyleOnCompile.value
      (Compile / compile).value
    },
    (Test / compile) := {
      scalaStyleOnTest.value
      (Test / compile).value
    }
  )

  lazy val compilerWarningSettings: Seq[sbt.Def.Setting[_]] = Seq(
    (Compile / scalacOptions) ++= {
      Seq(
        // replace -Xfatal-warnings with fine-grained configuration, since 2.13.2
        // verbose warning on deprecation, error on all others
        // see `scalac -Wconf:help` for details
        // since 2.13.15, "-Wconf:cat=deprecation:wv,any:e" no longer takes effect and needs to
        // be changed to "-Wconf:any:e", "-Wconf:cat=deprecation:wv",
        // please refer to the details: https://github.com/scala/scala/pull/10708
        "-Wconf:any:e",
        "-Wconf:cat=deprecation:wv",
        // 2.13-specific warning hits to be muted (as narrowly as possible) and addressed separately
        "-Wunused:imports",
        "-Wconf:msg=^(?=.*?method|value|type|object|trait|inheritance)(?=.*?deprecated)(?=.*?since 2.13).+$:e",
        "-Wconf:msg=^(?=.*?Widening conversion from)(?=.*?is deprecated because it loses precision).+$:e",
        // SPARK-45610 Convert "Auto-application to `()` is deprecated" to compile error, as it will become a compile error in Scala 3.
        "-Wconf:cat=deprecation&msg=Auto-application to \\`\\(\\)\\` is deprecated:e",
        // SPARK-35574 Prevent the recurrence of compilation warnings related to `procedure syntax is deprecated`
        "-Wconf:cat=deprecation&msg=procedure syntax is deprecated:e",
        // SPARK-45627 Symbol literals are deprecated in Scala 2.13 and it's a compile error in Scala 3.
        "-Wconf:cat=deprecation&msg=symbol literal is deprecated:e",
        // SPARK-45627 `enum`, `export` and `given` will become keywords in Scala 3,
        // so they are prohibited from being used as variable names in Scala 2.13 to
        // reduce the cost of migration in subsequent versions.
        "-Wconf:cat=deprecation&msg=it will become a keyword in Scala 3:e",
        // SPARK-46938 to prevent enum scan on pmml-model, under spark-mllib module.
        "-Wconf:cat=other&site=org.dmg.pmml.*:w",
        // SPARK-49937 ban call the method `SparkThrowable#getErrorClass`
        "-Wconf:cat=deprecation&msg=method getErrorClass in trait SparkThrowable is deprecated:e"
      )
    }
  )

  val noLintOnCompile = sys.env.contains("NOLINT_ON_COMPILE") &&
      !sys.env.get("NOLINT_ON_COMPILE").contains("false")
  lazy val sharedSettings = sparkGenjavadocSettings ++
                            compilerWarningSettings ++
      (if (noLintOnCompile) Nil else enableScalaStyle) ++ Seq(
    (Compile / exportJars) := true,
    (Test / exportJars) := false,
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home").map { p => new File(p).getParentFile().getAbsolutePath() })
      .map(file),
    publishMavenStyle := true,
    unidocGenjavadocVersion := "0.19",

    // Override SBT's default resolvers:
    resolvers := Seq(
      // Google Mirror of Maven Central, placed first so that it's used instead of flaky Maven Central.
      // See https://storage-download.googleapis.com/maven-central/index.html for more info.
      "gcs-maven-central-mirror" at "https://maven-central.storage-download.googleapis.com/maven2/",
      DefaultMavenRepository,
      Resolver.mavenLocal,
      Resolver.file("ivyLocal", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
    ),
    externalResolvers := resolvers.value,
    otherResolvers := SbtPomKeys.mvnLocalRepository(dotM2 => Seq(Resolver.file("dotM2", dotM2))).value,
    (MavenCompile / publishLocalConfiguration) := PublishConfiguration()
        .withResolverName("dotM2")
        .withArtifacts(packagedArtifacts.value.toVector)
        .withLogging(ivyLoggingLevel.value),
    (SbtCompile / publishLocalConfiguration) := PublishConfiguration()
        .withResolverName("ivyLocal")
        .withArtifacts(packagedArtifacts.value.toVector)
        .withLogging(ivyLoggingLevel.value),
    (MavenCompile / publishMavenStyle) := true,
    (SbtCompile / publishMavenStyle) := false,
    (MavenCompile / publishLocal) := publishOrSkip((MavenCompile / publishLocalConfiguration),
      (publishLocal / skip)).value,
    (SbtCompile / publishLocal) := publishOrSkip((SbtCompile / publishLocalConfiguration),
      (publishLocal / skip)).value,
    publishLocal := Seq((MavenCompile / publishLocal), (SbtCompile / publishLocal)).dependOn.value,

    javaOptions ++= {
      // for `dev.ludovic.netlib.blas` which implements such hardware-accelerated BLAS operations
      Seq("--add-modules=jdk.incubator.vector")
    },

    (Compile / doc / javacOptions) ++= {
      Seq("-Xdoclint:all", "-Xdoclint:-missing")
    },

    javaVersion := SbtPomKeys.effectivePom.value.getProperties.get("java.version").asInstanceOf[String],

    (Compile / javacOptions) ++= Seq(
      "-encoding", UTF_8.name(),
      "-g",
      "--release", javaVersion.value
    ),
    // This -target and Xlint:unchecked options cannot be set in the Compile configuration scope since
    // `javadoc` doesn't play nicely with them; see https://github.com/sbt/sbt/issues/355#issuecomment-3817629
    // for additional discussion and explanation.
    (Compile / compile / javacOptions) ++= Seq(
      "-Xlint:unchecked"
    ),

    (Compile / scalacOptions) ++= Seq(
      "-release", javaVersion.value,
      "-sourcepath", (ThisBuild / baseDirectory).value.getAbsolutePath  // Required for relative source links in scaladoc
    ),

    SbtPomKeys.profiles := profiles,

    // Remove certain packages from Scaladoc
    (Compile / doc / scalacOptions) := Seq(
      "-groups",
      "-skip-packages", Seq(
        "org.apache.spark.api.python",
        "org.apache.spark.network",
        "org.apache.spark.deploy",
        "org.apache.spark.util.collection",
        "org.apache.spark.sql.scripting"
      ).mkString(":"),
      "-doc-title", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    ),

    // disable Mima check for all modules,
    // to be enabled in specific ones that have previous artifacts
    MimaKeys.mimaFailOnNoPrevious := false,

    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := protoVersion,
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ optionallyEnabledProjects ++ assemblyProjects ++ copyJarsProjects ++ Seq(spark, tools))
    .foreach(enable(sharedSettings ++ DependencyOverrides.settings ++
      ExcludedDependencies.settings ++ Checkstyle.settings ++ ExcludeShims.settings))

  /* Enable tests settings for all projects except examples, assembly and tools */
  (allProjects ++ optionallyEnabledProjects).foreach(enable(TestSettings.settings))

  val mimaProjects = allProjects.filterNot { x =>
    Seq(
      spark, hive, hiveThriftServer, repl, networkCommon, networkShuffle, networkYarn,
      unsafe, tags, tokenProviderKafka010, sqlKafka010, connectCommon, connect, connectClient,
      variant, connectShims, profiler
    ).contains(x)
  }

  mimaProjects.foreach { x =>
    enable(MimaBuild.mimaSettings(sparkHome, x))(x)
  }

  /* Generate and pick the spark build info from extra-resources */
  enable(CommonUtils.settings)(commonUtils)

  enable(Core.settings)(core)

  /* Unsafe settings */
  enable(Unsafe.settings)(unsafe)

  /*
   * Set up tasks to copy dependencies during packaging. This step can be disabled in the command
   * line, so that dev/mima can run without trying to copy these files again and potentially
   * causing issues.
   */
  if (!"false".equals(System.getProperty("copyDependencies"))) {
    copyJarsProjects.foreach(enable(CopyDependencies.settings))
  }

  /* Enable Assembly for all assembly projects */
  assemblyProjects.foreach(enable(Assembly.settings))

  /* Package pyspark artifacts in a separate zip file for YARN. */
  enable(PySparkAssembly.settings)(assembly)

  /* Enable unidoc only for the root spark project */
  enable(SparkUnidoc.settings)(spark)

  /* Enable unidoc only for the root spark connect client project */
  // enable(SparkConnectClientUnidoc.settings)(connectClient)

  /* Sql-api ANTLR generation settings */
  enable(SqlApi.settings)(sqlApi)

  /* Spark SQL Core settings */
  enable(SQL.settings)(sql)

  /* Hive console settings */
  enable(Hive.settings)(hive)

  enable(SparkConnectCommon.settings)(connectCommon)
  enable(SparkConnect.settings)(connect)
  enable(SparkConnectClient.settings)(connectClient)

  /* Protobuf settings */
  enable(SparkProtobuf.settings)(protobuf)

  enable(DockerIntegrationTests.settings)(dockerIntegrationTests)

  enable(KubernetesIntegrationTests.settings)(kubernetesIntegrationTests)

  enable(YARN.settings)(yarn)

  if (profiles.contains("sparkr")) {
    enable(SparkR.settings)(core)
  }

  /**
   * Adds the ability to run the spark shell directly from SBT without building an assembly
   * jar.
   *
   * Usage: `build/sbt sparkShell`
   */
  val sparkShell = taskKey[Unit]("start a spark-shell.")
  val sparkPackage = inputKey[Unit](
    s"""
       |Download and run a spark package.
       |Usage `builds/sbt "sparkPackage <group:artifact:version> <MainClass> [args]
     """.stripMargin)
  val sparkSql = taskKey[Unit]("starts the spark sql CLI.")

  enable(Seq(
    (run / connectInput) := true,
    fork := true,
    (run / outputStrategy) := Some (StdoutOutput),

    javaOptions += "-Xmx2g",

    sparkShell := {
      (Compile / runMain).toTask(" org.apache.spark.repl.Main -usejavacp").value
    },

    sparkPackage := {
      import complete.DefaultParsers._
      val packages :: className :: otherArgs = spaceDelimited("<group:artifact:version> <MainClass> [args]").parsed.toList
      val scalaRun = (run / runner).value
      val classpath = (Runtime / fullClasspath).value
      val args = Seq("--packages", packages, "--class", className, (LocalProject("core") / Compile / Keys.`package`)
        .value.getCanonicalPath) ++ otherArgs
      println(args)
      scalaRun.run("org.apache.spark.deploy.SparkSubmit", classpath.map(_.data), args, streams.value.log)
    },

    (Compile / javaOptions) += "-Dspark.master=local",

    sparkSql := {
      (Compile / runMain).toTask(" org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver").value
    }
  ))(assembly)

  enable(Seq(sparkShell := (LocalProject("assembly") / sparkShell).value))(spark)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    } ++ Seq[Project](OldDeps.project)
  }

  if (!sys.env.contains("SERIAL_SBT_TESTS")) {
    allProjects.foreach(enable(SparkParallelTestGrouping.settings))
  }
}

object SparkParallelTestGrouping {
  // Settings for parallelizing tests. The basic strategy here is to run the slowest suites (or
  // collections of suites) in their own forked JVMs, allowing us to gain parallelism within a
  // SBT project. Here, we take an opt-in approach where the default behavior is to run all
  // tests sequentially in a single JVM, requiring us to manually opt-in to the extra parallelism.
  //
  // There are a reasons why such an opt-in approach is good:
  //
  //    1. Launching one JVM per suite adds significant overhead for short-running suites. In
  //       addition to JVM startup time and JIT warmup, it appears that initialization of Derby
  //       metastores can be very slow so creating a fresh warehouse per suite is inefficient.
  //
  //    2. When parallelizing within a project we need to give each forked JVM a different tmpdir
  //       so that the metastore warehouses do not collide. Unfortunately, it seems that there are
  //       some tests which have an overly tight dependency on the default tmpdir, so those fragile
  //       tests need to continue re-running in the default configuration (or need to be rewritten).
  //       Fixing that problem would be a huge amount of work for limited payoff in most cases
  //       because most test suites are short-running.
  //

  private val testsWhichShouldRunInTheirOwnDedicatedJvm = Set(
    "org.apache.spark.DistributedSuite",
    "org.apache.spark.scheduler.HealthTrackerIntegrationSuite",
    "org.apache.spark.sql.catalyst.expressions.DateExpressionsSuite",
    "org.apache.spark.sql.catalyst.expressions.HashExpressionsSuite",
    "org.apache.spark.sql.catalyst.expressions.CastSuite",
    "org.apache.spark.sql.catalyst.expressions.MathExpressionsSuite",
    "org.apache.spark.sql.hive.HiveExternalCatalogSuite",
    "org.apache.spark.sql.hive.StatisticsSuite",
    "org.apache.spark.sql.hive.client.HiveClientVersions",
    "org.apache.spark.sql.hive.HiveExternalCatalogVersionsSuite",
    "org.apache.spark.ml.classification.LogisticRegressionSuite",
    "org.apache.spark.ml.classification.LinearSVCSuite",
    "org.apache.spark.sql.SQLQueryTestSuite",
    "org.apache.spark.sql.hive.client.HadoopVersionInfoSuite",
    "org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperationSuite",
    "org.apache.spark.sql.hive.thriftserver.ThriftServerQueryTestSuite",
    "org.apache.spark.sql.hive.thriftserver.SparkSQLEnvSuite",
    "org.apache.spark.sql.hive.thriftserver.ui.ThriftServerPageSuite",
    "org.apache.spark.sql.hive.thriftserver.ui.HiveThriftServer2ListenerSuite",
    "org.apache.spark.sql.kafka010.KafkaDelegationTokenSuite",
    "org.apache.spark.sql.streaming.RocksDBStateStoreStreamingAggregationSuite",
    "org.apache.spark.shuffle.KubernetesLocalDiskShuffleDataIOSuite",
    "org.apache.spark.sql.hive.HiveScalaReflectionSuite"
  ) ++ sys.env.get("DEDICATED_JVM_SBT_TESTS").map(_.split(",")).getOrElse(Array.empty).toSet

  private val DEFAULT_TEST_GROUP = "default_test_group"
  private val HIVE_EXECUTION_TEST_GROUP = "hive_execution_test_group"

  private def testNameToTestGroup(name: String): String = name match {
    case _ if testsWhichShouldRunInTheirOwnDedicatedJvm.contains(name) => name
    // Different with the cases in testsWhichShouldRunInTheirOwnDedicatedJvm, here we are grouping
    // all suites of `org.apache.spark.sql.hive.execution.*` into a single group, instead of
    // launching one JVM per suite.
    case _ if name.contains("org.apache.spark.sql.hive.execution") => HIVE_EXECUTION_TEST_GROUP
    case _ => DEFAULT_TEST_GROUP
  }

  lazy val settings = Seq(
    (Test / testGrouping) := {
      val tests: Seq[TestDefinition] = (Test / definedTests).value
      val defaultForkOptions = ForkOptions(
        javaHome = javaHome.value,
        outputStrategy = outputStrategy.value,
        bootJars = Vector.empty[java.io.File],
        workingDirectory = Some(baseDirectory.value),
        runJVMOptions = (Test / javaOptions).value.toVector,
        connectInput = connectInput.value,
        envVars = (Test / envVars).value
      )
      tests.groupBy(test => testNameToTestGroup(test.name)).map { case (groupName, groupTests) =>
        val forkOptions = {
          if (groupName == DEFAULT_TEST_GROUP) {
            defaultForkOptions
          } else {
            defaultForkOptions.withRunJVMOptions(defaultForkOptions.runJVMOptions ++
              Seq(s"-Djava.io.tmpdir=${baseDirectory.value}/target/tmp/$groupName"))
          }
        }
        new Tests.Group(
          name = groupName,
          tests = groupTests,
          runPolicy = Tests.SubProcess(forkOptions))
      }
    }.toSeq
  )
}

object CommonUtils {
  import scala.sys.process.Process
  def buildenv = Process(Seq("uname")).!!.trim.replaceFirst("[^A-Za-z0-9].*", "").toLowerCase
  def bashpath = Process(Seq("where", "bash")).!!.split("[\r\n]+").head.replace('\\', '/')
  lazy val settings = Seq(
    (Compile / resourceGenerators) += Def.task {
      val buildScript = baseDirectory.value + "/../../build/spark-build-info"
      val targetDir = baseDirectory.value + "/target/extra-resources/"
      // support Windows build under cygwin/mingw64, etc
      val bash = buildenv match {
        case "cygwin" | "msys2" | "mingw64" | "clang64" => bashpath
        case _ => "bash"
      }
      val command = Seq(bash, buildScript, targetDir, version.value)
      Process(command).!!
      val propsFile = baseDirectory.value / "target" / "extra-resources" / "spark-version-info.properties"
      Seq(propsFile)
    }.taskValue
  )
}

object Core {
  import BuildCommons.protoVersion
  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    (Compile / PB.targets) := Seq(
      PB.gens.java -> (Compile / sourceManaged).value
    )
  ) ++ {
    val sparkProtocExecPath = sys.props.get("spark.protoc.executable.path")
    if (sparkProtocExecPath.isDefined) {
      Seq(
        PB.protocExecutable := file(sparkProtocExecPath.get)
      )
    } else {
      Seq.empty
    }
  }
}

object SparkConnectCommon {
  import BuildCommons.protoVersion

  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,

    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "connect.guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      val grpcVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "io.grpc.version").asInstanceOf[String]
      Seq(
        "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin(),
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },

    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "connect.guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    (assembly / test) := { },

    (assembly / logLevel) := Level.Info,

    // Exclude `scala-library` from assembly.
    (assembly / assemblyPackageScala / assembleArtifact) := false,

    // Exclude `pmml-model-*.jar`, `scala-collection-compat_*.jar`,`jsr305-*.jar` and
    // `netty-*.jar` and `unused-1.0.0.jar` from assembly.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      cp filter { v =>
        val name = v.data.getName
        name.startsWith("pmml-model-") || name.startsWith("scala-collection-compat_") ||
          name.startsWith("jsr305-") || name.startsWith("netty-") || name == "unused-1.0.0.jar"
      }
    },

    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      // Drop all proto files that are not needed as artifacts of the build.
      case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  ) ++ {
    val sparkProtocExecPath = sys.props.get("spark.protoc.executable.path")
    val connectPluginExecPath = sys.props.get("connect.plugin.executable.path")
    if (sparkProtocExecPath.isDefined && connectPluginExecPath.isDefined) {
      Seq(
        (Compile / PB.targets) := Seq(
          PB.gens.java -> (Compile / sourceManaged).value,
          PB.gens.plugin(name = "grpc-java", path = connectPluginExecPath.get) -> (Compile / sourceManaged).value
        ),
        PB.protocExecutable := file(sparkProtocExecPath.get)
      )
    } else {
      Seq(
        (Compile / PB.targets) := Seq(
          PB.gens.java -> (Compile / sourceManaged).value,
          PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value
        )
      )
    }
  }
}

object SparkConnect {
  import BuildCommons.protoVersion

  lazy val settings = Seq(
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "connect.guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },

    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "connect.guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    (assembly / test) := { },

    (assembly / logLevel) := Level.Info,

    // Exclude `scala-library` from assembly.
    (assembly / assemblyPackageScala / assembleArtifact) := false,

    // SPARK-46733: Include `spark-connect-*.jar`, `unused-*.jar`,`guava-*.jar`,
    // `failureaccess-*.jar`, `annotations-*.jar`, `grpc-*.jar`, `protobuf-*.jar`,
    // `gson-*.jar`, `error_prone_annotations-*.jar`, `j2objc-annotations-*.jar`,
    // `animal-sniffer-annotations-*.jar`, `perfmark-api-*.jar`,
    // `proto-google-common-protos-*.jar` in assembly.
    // This needs to be consistent with the content of `maven-shade-plugin`.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      val validPrefixes = Set("spark-connect", "unused-", "guava-", "failureaccess-",
        "annotations-", "grpc-", "protobuf-", "gson", "error_prone_annotations",
        "j2objc-annotations", "animal-sniffer-annotations", "perfmark-api",
        "proto-google-common-protos")
      cp filterNot { v =>
        validPrefixes.exists(v.data.getName.startsWith)
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("io.grpc.**" -> "org.sparkproject.connect.grpc.@0").inAll,
      ShadeRule.rename("com.google.common.**" -> "org.sparkproject.connect.guava.@1").inAll,
      ShadeRule.rename("com.google.thirdparty.**" -> "org.sparkproject.connect.guava.@1").inAll,
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.connect.protobuf.@1").inAll,
      ShadeRule.rename("android.annotation.**" -> "org.sparkproject.connect.android_annotation.@1").inAll,
      ShadeRule.rename("io.perfmark.**" -> "org.sparkproject.connect.io_perfmark.@1").inAll,
      ShadeRule.rename("org.codehaus.mojo.animal_sniffer.**" -> "org.sparkproject.connect.animal_sniffer.@1").inAll,
      ShadeRule.rename("com.google.j2objc.annotations.**" -> "org.sparkproject.connect.j2objc_annotations.@1").inAll,
      ShadeRule.rename("com.google.errorprone.annotations.**" -> "org.sparkproject.connect.errorprone_annotations.@1").inAll,
      ShadeRule.rename("org.checkerframework.**" -> "org.sparkproject.connect.checkerframework.@1").inAll,
      ShadeRule.rename("com.google.gson.**" -> "org.sparkproject.connect.gson.@1").inAll,
      ShadeRule.rename("com.google.api.**" -> "org.sparkproject.connect.google_protos.api.@1").inAll,
      ShadeRule.rename("com.google.cloud.**" -> "org.sparkproject.connect.google_protos.cloud.@1").inAll,
      ShadeRule.rename("com.google.geo.**" -> "org.sparkproject.connect.google_protos.geo.@1").inAll,
      ShadeRule.rename("com.google.logging.**" -> "org.sparkproject.connect.google_protos.logging.@1").inAll,
      ShadeRule.rename("com.google.longrunning.**" -> "org.sparkproject.connect.google_protos.longrunning.@1").inAll,
      ShadeRule.rename("com.google.rpc.**" -> "org.sparkproject.connect.google_protos.rpc.@1").inAll,
      ShadeRule.rename("com.google.type.**" -> "org.sparkproject.connect.google_protos.type.@1").inAll
    ),

    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      // Drop all proto files that are not needed as artifacts of the build.
      case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
}

object SparkConnectClient {
  import BuildCommons.protoVersion
  val buildTestDeps = TaskKey[Unit]("buildTestDeps", "Build needed dependencies for test.")

  lazy val settings = Seq(
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "connect.guava.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "connect.guava.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    buildTestDeps := {
      (LocalProject("assembly") / Compile / Keys.`package`).value
      (LocalProject("catalyst") / Test / Keys.`package`).value
    },

    // SPARK-42538: Make sure the `${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars` is available for testing.
    // At the same time, the build of `connect`, `connect-client-jvm` and `sql` will be triggered by `assembly` build,
    // so no additional configuration is required.
    test := ((Test / test) dependsOn (buildTestDeps)).value,

    testOnly := ((Test / testOnly) dependsOn (buildTestDeps)).evaluated,

    (assembly / test) := { },

    (assembly / logLevel) := Level.Info,

    // Exclude `scala-library` from assembly.
    (assembly / assemblyPackageScala / assembleArtifact) := false,

    // Exclude `pmml-model-*.jar`, `scala-collection-compat_*.jar`,`jsr305-*.jar` and
    // `netty-*.jar` and `unused-1.0.0.jar` from assembly.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      cp filter { v =>
        val name = v.data.getName
        name.startsWith("pmml-model-") || name.startsWith("scala-collection-compat_") ||
          name.startsWith("jsr305-") || name == "unused-1.0.0.jar"
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("io.grpc.**" -> "org.sparkproject.connect.client.io.grpc.@1").inAll,
      ShadeRule.rename("com.google.**" -> "org.sparkproject.connect.client.com.google.@1").inAll,
      ShadeRule.rename("io.netty.**" -> "org.sparkproject.connect.client.io.netty.@1").inAll,
      ShadeRule.rename("org.checkerframework.**" -> "org.sparkproject.connect.client.org.checkerframework.@1").inAll,
      ShadeRule.rename("javax.annotation.**" -> "org.sparkproject.connect.client.javax.annotation.@1").inAll,
      ShadeRule.rename("io.perfmark.**" -> "org.sparkproject.connect.client.io.perfmark.@1").inAll,
      ShadeRule.rename("org.codehaus.**" -> "org.sparkproject.connect.client.org.codehaus.@1").inAll,
      ShadeRule.rename("android.annotation.**" -> "org.sparkproject.connect.client.android.annotation.@1").inAll
    ),

    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      // Drop all proto files that are not needed as artifacts of the build.
      case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
}

object SparkProtobuf {
  import BuildCommons.protoVersion

  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,

    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",

    dependencyOverrides += "com.google.protobuf" % "protobuf-java" % protoVersion,

    (Test / PB.protoSources) += (Test / sourceDirectory).value / "resources" / "protobuf",

    (Test / PB.protocOptions) += "--include_imports",

    (Test / PB.targets) := Seq(
      PB.gens.java -> target.value / "generated-test-sources",
      PB.gens.descriptorSet -> target.value / "generated-test-sources/descriptor-set-sbt.desc",
      // The above creates single descriptor file with all the proto files. This is different from
      // Maven, which create one descriptor file for each proto file.
    ),

    (assembly / test) := { },

    (assembly / logLevel) := Level.Info,

    // Exclude `scala-library` from assembly.
    (assembly / assemblyPackageScala / assembleArtifact) := false,

    // Exclude `pmml-model-*.jar`, `scala-collection-compat_*.jar`,
    // `spark-tags_*.jar`, "guava-*.jar" and `unused-1.0.0.jar` from assembly.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      cp filter { v =>
        val name = v.data.getName
        name.startsWith("pmml-model-") || name.startsWith("scala-collection-compat_") ||
          name.startsWith("spark-tags_") || name.startsWith("guava-") || name == "unused-1.0.0.jar"
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.spark_protobuf.protobuf.@1").inAll,
    ),

    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      // Drop all proto files that are not needed as artifacts of the build.
      case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
  ) ++ {
    val sparkProtocExecPath = sys.props.get("spark.protoc.executable.path")
    if (sparkProtocExecPath.isDefined) {
      Seq(
        PB.protocExecutable := file(sparkProtocExecPath.get)
      )
    } else {
      Seq.empty
    }
  }
}

object Unsafe {
  lazy val settings = Seq(
    // This option is needed to suppress warnings from sun.misc.Unsafe usage
    (Compile / javacOptions) += "-XDignore.symbol.file"
  )
}


object DockerIntegrationTests {
  // This serves to override the override specified in DependencyOverrides:
  lazy val settings = Seq(
    dependencyOverrides += "com.google.guava" % "guava" % "33.1.0-jre"
  )
}

/**
 * These settings run the Kubernetes integration tests.
 * Docker images will have the "dev" tag, and will be overwritten every time the
 * integration tests are run. The integration tests are actually bound to the "test" phase,
 * so running "test" on this module will run the integration tests.
 *
 * There are two ways to run the tests:
 * - the "tests" task builds docker images and runs the test, so it's a little slow.
 * - the "run-its" task just runs the tests on a pre-built set of images.
 *
 * Note that this does not use the shell scripts that the maven build uses, which are more
 * configurable. This is meant as a quick way for developers to run these tests against their
 * local changes.
 */
object KubernetesIntegrationTests {
  import BuildCommons._
  import scala.sys.process.Process

  val dockerBuild = TaskKey[Unit]("docker-imgs", "Build the docker images for ITs.")
  val runITs = TaskKey[Unit]("run-its", "Only run ITs, skip image build.")
  val imageRepo = sys.props.getOrElse("spark.kubernetes.test.imageRepo", "docker.io/kubespark")
  val imageTag = sys.props.get("spark.kubernetes.test.imageTag")
  val namespace = sys.props.get("spark.kubernetes.test.namespace")
  val deployMode = sys.props.get("spark.kubernetes.test.deployMode")

  // Hack: this variable is used to control whether to build docker images. It's updated by
  // the tasks below in a non-obvious way, so that you get the functionality described in
  // the scaladoc above.
  private var shouldBuildImage = true

  lazy val settings = Seq(
    dockerBuild := {
      if (shouldBuildImage) {
        val dockerTool = s"$sparkHome/bin/docker-image-tool.sh"
        val bindingsDir = s"$sparkHome/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings"
        val javaImageTag = sys.props.get("spark.kubernetes.test.javaImageTag")
        val dockerFile = sys.props.getOrElse("spark.kubernetes.test.dockerFile",
            s"$sparkHome/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile")
        val pyDockerFile = sys.props.getOrElse("spark.kubernetes.test.pyDockerFile",
            s"$bindingsDir/python/Dockerfile")
        var rDockerFile = sys.props.getOrElse("spark.kubernetes.test.rDockerFile",
            s"$bindingsDir/R/Dockerfile")
        val excludeTags = sys.props.getOrElse("test.exclude.tags", "").split(",")
        if (excludeTags.exists(_.equalsIgnoreCase("r"))) {
          rDockerFile = ""
        }
        val extraOptions = if (javaImageTag.isDefined) {
          Seq("-b", s"java_image_tag=$javaImageTag")
        } else {
          Seq("-f", s"$dockerFile")
        }
        val cmd = Seq(dockerTool,
          "-r", imageRepo,
          "-t", imageTag.getOrElse("dev"),
          "-p", pyDockerFile,
          "-R", rDockerFile) ++
          (if (deployMode != Some("minikube")) Seq.empty else Seq("-m")) ++
          extraOptions :+
          "build"
        val ec = Process(cmd).!
        if (ec != 0) {
          throw new IllegalStateException(s"Process '${cmd.mkString(" ")}' exited with $ec.")
        }
        if (deployMode == Some("cloud")) {
          val cmd = Seq(dockerTool, "-r", imageRepo, "-t", imageTag.getOrElse("dev"), "push")
          val ret = Process(cmd).!
          if (ret != 0) {
            throw new IllegalStateException(s"Process '${cmd.mkString(" ")}' exited with $ret.")
          }
        }
      }
      shouldBuildImage = true
    },
    runITs := Def.taskDyn {
      shouldBuildImage = false
      Def.task {
        (Test / test).value
      }
    }.value,
    (Test / test) := (Test / test).dependsOn(dockerBuild).value,
    (Test / javaOptions) ++= Seq(
      s"-Dspark.kubernetes.test.deployMode=${deployMode.getOrElse("minikube")}",
      s"-Dspark.kubernetes.test.imageRepo=${imageRepo}",
      s"-Dspark.kubernetes.test.imageTag=${imageTag.getOrElse("dev")}",
      s"-Dspark.kubernetes.test.unpackSparkDir=$sparkHome"
    ),
    (Test / javaOptions) ++= namespace.map("-Dspark.kubernetes.test.namespace=" + _),
    // Force packaging before building images, so that the latest code is tested.
    dockerBuild := dockerBuild
      .dependsOn(assembly / Compile / packageBin)
      .dependsOn(examples / Compile / packageBin)
      .value
  )
}

/**
 * Overrides to work around sbt's dependency resolution being different from Maven's.
 */
object DependencyOverrides {
  lazy val guavaVersion = sys.props.get("guava.version").getOrElse("33.3.1-jre")
  lazy val settings = Seq(
    dependencyOverrides += "com.google.guava" % "guava" % guavaVersion,
    dependencyOverrides += "jline" % "jline" % "2.14.6",
    dependencyOverrides += "org.apache.avro" % "avro" % "1.12.0")
}

/**
 * This excludes library dependencies in sbt, which are specified in maven but are
 * not needed by sbt build.
 */
object ExcludedDependencies {
  lazy val settings = Seq(
    libraryDependencies ~= { libs => libs.filterNot(_.name == "groovy-all") },
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "ch.qos.logback"),
      ExclusionRule("javax.servlet", "javax.servlet-api"))
  )
}

/**
 * This excludes the spark-connect-shims module from a module when it is not part of the connect
 * client dependencies.
 */
object ExcludeShims {
  val shimmedProjects = Set("spark-sql-api", "spark-connect-common", "spark-connect-client-jvm")
  val classPathFilter = TaskKey[Classpath => Classpath]("filter for classpath")
  lazy val settings = Seq(
    classPathFilter := {
      if (!shimmedProjects(moduleName.value)) {
        cp => cp.filterNot(_.data.name.contains("spark-connect-shims"))
      } else {
        identity _
      }
    },
    Compile / internalDependencyClasspath :=
      classPathFilter.value((Compile / internalDependencyClasspath).value),
    Compile / internalDependencyAsJars :=
      classPathFilter.value((Compile / internalDependencyAsJars).value),
    Runtime / internalDependencyClasspath :=
      classPathFilter.value((Runtime / internalDependencyClasspath).value),
    Runtime / internalDependencyAsJars :=
      classPathFilter.value((Runtime / internalDependencyAsJars).value),
    Test / internalDependencyClasspath :=
      classPathFilter.value((Test / internalDependencyClasspath).value),
    Test / internalDependencyAsJars :=
      classPathFilter.value((Test / internalDependencyAsJars).value),
  )
}

/**
 * Project to pull previous artifacts of Spark for generating Mima excludes.
 */
object OldDeps {

  import BuildCommons.protoVersion

  lazy val project = Project("oldDeps", file("dev"))
    .settings(oldDepsSettings)
    .disablePlugins(sbtpomreader.PomReaderPlugin)

  lazy val allPreviousArtifactKeys = Def.settingDyn[Seq[Set[ModuleID]]] {
    SparkBuild.mimaProjects
      .map { project => (project / MimaKeys.mimaPreviousArtifacts) }
      .map(k => Def.setting(k.value))
      .join
  }

  def oldDepsSettings() = Defaults.coreDefaultSettings ++ Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := protoVersion,
    name := "old-deps",
    libraryDependencies := allPreviousArtifactKeys.value.flatten
  )
}

object SqlApi {
  import com.simplytyped.Antlr4Plugin
  import com.simplytyped.Antlr4Plugin.autoImport._

  lazy val settings = Antlr4Plugin.projectSettings ++ Seq(
    (Antlr4 / antlr4Version) := SbtPomKeys.effectivePom.value.getProperties.get("antlr4.version").asInstanceOf[String],
    (Antlr4 / antlr4PackageName) := Some("org.apache.spark.sql.catalyst.parser"),
    (Antlr4 / antlr4GenListener) := true,
    (Antlr4 / antlr4GenVisitor) := true,
    (Antlr4 / antlr4TreatWarningsAsErrors) := true
  )
}

object SQL {
  import BuildCommons.protoVersion
  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    (Compile / PB.targets) := Seq(
      PB.gens.java -> (Compile / sourceManaged).value
    )
  ) ++ {
    val sparkProtocExecPath = sys.props.get("spark.protoc.executable.path")
    if (sparkProtocExecPath.isDefined) {
      Seq(
        PB.protocExecutable := file(sparkProtocExecPath.get)
      )
    } else {
      Seq.empty
    }
  }
}

object Hive {

  lazy val settings = Seq(
    // Specially disable assertions since some Hive tests fail them
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_ == "-ea"),
    // Hive tests need higher metaspace size
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_.contains("MaxMetaspaceSize")),
    (Test / javaOptions) += "-XX:MaxMetaspaceSize=2g",
    // SPARK-45265: HivePartitionFilteringSuite addPartitions related tests generate supper long
    // direct sql against derby server, which may cause stack overflow error when derby do sql
    // parsing.
    // We need to increase the Xss for the test. Meanwhile, QueryParsingErrorsSuite requires a
    // smaller size of Xss to mock a FAILED_TO_PARSE_TOO_COMPLEX error, so we need to set for
    // hive moudle specifically.
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_.contains("Xss")),
    // SPARK-45265: The value for `-Xss` should be consistent with the configuration value for
    // `scalatest-maven-plugin` in `sql/hive/pom.xml`
    (Test / javaOptions) += "-Xss64m",
    // Supporting all SerDes requires us to depend on deprecated APIs, so we turn off the warnings
    // only for this subproject.
    scalacOptions := (scalacOptions map { currentOpts: Seq[String] =>
      currentOpts.filterNot(_ == "-deprecation")
    }).value,
    // Some of our log4j jars make it impossible to submit jobs from this JVM to Hive Map/Reduce
    // in order to generate golden files.  This is only required for developers who are adding new
    // new query tests.
    (Test / fullClasspath) := (Test / fullClasspath).value.filterNot { f => f.toString.contains("jcl-over") }
  )
}

object YARN {
  val genConfigProperties = TaskKey[Unit]("gen-config-properties",
    "Generate config.properties which contains a setting whether Hadoop is provided or not")
  val propFileName = "config.properties"
  val hadoopProvidedProp = "spark.yarn.isHadoopProvided"

  lazy val settings = Seq(
    Compile / unmanagedResources :=
      (Compile / unmanagedResources).value.filter(!_.getName.endsWith(s"$propFileName")),
    genConfigProperties := {
      val file = (Compile / classDirectory).value / s"org/apache/spark/deploy/yarn/$propFileName"
      val isHadoopProvided = SbtPomKeys.effectivePom.value.getProperties.get(hadoopProvidedProp)
      sbt.IO.write(file, s"$hadoopProvidedProp = $isHadoopProvided")
    },
    Compile / copyResources := (Def.taskDyn {
      val c = (Compile / copyResources).value
      Def.task {
        (Compile / genConfigProperties).value
        c
      }
    }).value
  )
}

object Assembly {
  import sbtassembly.AssemblyPlugin.autoImport._

  val hadoopVersion = taskKey[String]("The version of hadoop that spark is compiled against.")

  lazy val settings = baseAssemblySettings ++ Seq(
    (assembly / test) := {},
    hadoopVersion := {
      sys.props.get("hadoop.version")
        .getOrElse(SbtPomKeys.effectivePom.value.getProperties.get("hadoop.version").asInstanceOf[String])
    },
    (assembly / assemblyJarName) := {
      lazy val hadoopVersionValue = hadoopVersion.value
      if (moduleName.value.contains("streaming-kafka-0-10-assembly")
        || moduleName.value.contains("streaming-kinesis-asl-assembly")) {
        s"${moduleName.value}-${version.value}.jar"
      } else {
        s"${moduleName.value}-${version.value}-hadoop${hadoopVersionValue}.jar"
      }
    },
    (Test / assembly / assemblyJarName) := s"${moduleName.value}-test-${version.value}.jar",
    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf")
                                                               => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).matches("meta-inf.*\\.sf$")
                                                               => MergeStrategy.discard
      case "log4j2.properties"                                 => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/services/")
                                                               => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )
}

object PySparkAssembly {
  import sbtassembly.AssemblyPlugin.autoImport._
  import java.util.zip.{ZipOutputStream, ZipEntry}

  lazy val settings = Seq(
    // Use a resource generator to copy all .py files from python/pyspark into a managed directory
    // to be included in the assembly. We can't just add "python/" to the assembly's resource dir
    // list since that will copy unneeded / unwanted files.
    (Compile / resourceGenerators) += Def.macroValueI((Compile / resourceManaged) map { outDir: File =>
      val src = new File(BuildCommons.sparkHome, "python/pyspark")
      val zipFile = new File(BuildCommons.sparkHome , "python/lib/pyspark.zip")
      zipFile.delete()
      zipRecursive(src, zipFile)
      Seq.empty[File]
    }).value
  )

  private def zipRecursive(source: File, destZipFile: File) = {
    val destOutput = new ZipOutputStream(new FileOutputStream(destZipFile))
    addFilesToZipStream("", source, destOutput)
    destOutput.flush()
    destOutput.close()
  }

  private def addFilesToZipStream(parent: String, source: File, output: ZipOutputStream): Unit = {
    if (source.isDirectory()) {
      output.putNextEntry(new ZipEntry(parent + source.getName()))
      for (file <- source.listFiles()) {
        addFilesToZipStream(parent + source.getName() + File.separator, file, output)
      }
    } else {
      val in = new FileInputStream(source)
      output.putNextEntry(new ZipEntry(parent + source.getName()))
      val buf = new Array[Byte](8192)
      var n = 0
      while (n != -1) {
        n = in.read(buf)
        if (n != -1) {
          output.write(buf, 0, n)
        }
      }
      output.closeEntry()
      in.close()
    }
  }

}

object SparkR {
  import scala.sys.process.Process

  val buildRPackage = taskKey[Unit]("Build the R package")
  lazy val settings = Seq(
    buildRPackage := {
      val postfix = if (File.separator == "\\") ".bat" else ".sh"
      val command = baseDirectory.value / ".." / "R" / s"install-dev$postfix"
      Process(command.toString).!!
    },
    (Compile / compile) := (Def.taskDyn {
      val c = (Compile / compile).value
      Def.task {
        (Compile / buildRPackage).value
        c
      }
    }).value
  )
}

trait SharedUnidocSettings {

  import BuildCommons._
  import sbtunidoc.BaseUnidocPlugin
  import sbtunidoc.JavaUnidocPlugin
  import sbtunidoc.ScalaUnidocPlugin
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import sbtunidoc.JavaUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport._

  protected def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages
      .map(_.filterNot(_.getName.contains("$")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/deploy")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/examples")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/internal")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/memory")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/network")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/rpc")))
      .map(_.filterNot(f =>
        f.getCanonicalPath.contains("org/apache/spark/shuffle") &&
        !f.getCanonicalPath.contains("org/apache/spark/shuffle/api")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/executor")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/ExecutorAllocationClient")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend")))
      .map(_.filterNot(f =>
        f.getCanonicalPath.contains("org/apache/spark/unsafe") &&
        !f.getCanonicalPath.contains("org/apache/spark/unsafe/types/CalendarInterval")))
      .map(_.filterNot(_.getCanonicalPath.contains("python")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/util/collection")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/util/io")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/util/kvstore")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/catalyst")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/connect/")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/classic/")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/execution")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/hive")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/catalog/v2/utils")))
      .map(_.filterNot(_.getCanonicalPath.contains("org.apache.spark.errors")))
      .map(_.filterNot(_.getCanonicalPath.contains("org.apache.spark.sql.errors")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/hive")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/v2/avro")))
      .map(_.filterNot(_.getCanonicalPath.contains("SSLOptions")))
  }

  private def ignoreClasspaths(classpaths: Seq[Classpath]): Seq[Classpath] = {
    classpaths
      .map(_.filterNot(_.data.getCanonicalPath.matches(""".*kafka-clients-0\.10.*""")))
      .map(_.filterNot(_.data.getCanonicalPath.matches(""".*kafka_2\..*-0\.10.*""")))
      .map(_.filterNot(_.data.getCanonicalPath.contains("apache-rat")))
      .map(_.filterNot(_.data.getCanonicalPath.contains("connect-shims")))
  }

  val unidocSourceBase = settingKey[String]("Base URL of source links in Scaladoc.")

  lazy val baseSettings = BaseUnidocPlugin.projectSettings ++
                      ScalaUnidocPlugin.projectSettings ++
                      JavaUnidocPlugin.projectSettings ++
                      Seq (
    publish := {},

    (ScalaUnidoc / unidoc / unidocAllClasspaths) := {
      ignoreClasspaths((ScalaUnidoc / unidoc / unidocAllClasspaths).value)
    },

    (JavaUnidoc / unidoc / unidocAllClasspaths) := {
      ignoreClasspaths((JavaUnidoc / unidoc / unidocAllClasspaths).value)
    },

    // Skip actual catalyst, but include the subproject.
    // Catalyst is not public API and contains quasiquotes which break scaladoc.
    (ScalaUnidoc / unidoc / unidocAllSources) := {
      ignoreUndocumentedPackages((ScalaUnidoc / unidoc / unidocAllSources).value)
    },

    // Skip class names containing $ and some internal packages in Javadocs
    (JavaUnidoc / unidoc / unidocAllSources) := {
      ignoreUndocumentedPackages((JavaUnidoc / unidoc / unidocAllSources).value)
        .map(_.filterNot(_.getCanonicalPath.contains("org/apache/hadoop")))
    },

    (JavaUnidoc / unidoc / javacOptions) := {
      Seq(
        "-windowtitle", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
        "-public",
        "-noqualifier", "java.lang",
        "-tag", """example:a:Example\:""",
        "-tag", """note:a:Note\:""",
        "-tag", "group:X",
        "-tag", "tparam:X",
        "-tag", "constructor:X",
        "-tag", "todo:X",
        "-tag", "groupname:X",
        "-tag", "inheritdoc",
        "--ignore-source-errors", "-notree"
      )
    },

    // Use GitHub repository for Scaladoc source links
    unidocSourceBase := s"https://github.com/apache/spark/tree/v${version.value}",

    (ScalaUnidoc / unidoc / scalacOptions) ++= Seq(
      "-groups", // Group similar methods together based on the @group annotation.
      "-skip-packages", "org.apache.hadoop",
      "-sourcepath", (ThisBuild / baseDirectory).value.getAbsolutePath
    ) ++ (
      // Add links to sources when generating Scaladoc for a non-snapshot release
      if (!isSnapshot.value) {
        Opts.doc.sourceUrl(unidocSourceBase.value + "€{FILE_PATH}.scala")
      } else {
        Seq()
      }
    )
  )
}

object SparkUnidoc extends SharedUnidocSettings {

  import BuildCommons._
  import sbtunidoc.BaseUnidocPlugin
  import sbtunidoc.JavaUnidocPlugin
  import sbtunidoc.ScalaUnidocPlugin
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import sbtunidoc.JavaUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport._

  override def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    super.ignoreUndocumentedPackages(packages)
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/internal")))
  }

  lazy val settings = baseSettings ++ Seq(
    (ScalaUnidoc / unidoc / unidocProjectFilter) :=
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, kubernetes,
        yarn, tags, streamingKafka010, sqlKafka010, connectCommon, connect, connectClient,
        connectShims, protobuf, profiler),
    (JavaUnidoc / unidoc / unidocProjectFilter) :=
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, kubernetes,
        yarn, tags, streamingKafka010, sqlKafka010, connectCommon, connect, connectClient,
        connectShims, protobuf, profiler),
  )
}

object SparkConnectClientUnidoc extends SharedUnidocSettings {

  import BuildCommons._
  import sbtunidoc.BaseUnidocPlugin
  import sbtunidoc.JavaUnidocPlugin
  import sbtunidoc.ScalaUnidocPlugin
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import sbtunidoc.JavaUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport._

  override def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    super.ignoreUndocumentedPackages(packages)
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/application")))
  }

  lazy val settings = baseSettings ++ Seq(
    (ScalaUnidoc / unidoc / unidocProjectFilter) := inProjects(connectClient, connectCommon, sqlApi),
    (JavaUnidoc / unidoc / unidocProjectFilter) := inProjects(connectClient, connectCommon, sqlApi),
  )
}

object Checkstyle {
  lazy val settings = Seq(
    checkstyleSeverityLevel := CheckstyleSeverityLevel.Error,
    (Compile / checkstyle / javaSource) := baseDirectory.value / "src/main/java",
    (Test / checkstyle / javaSource) := baseDirectory.value / "src/test/java",
    checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
    checkstyleOutputFile := baseDirectory.value / "target/checkstyle-output.xml",
    (Test / checkstyleOutputFile) := baseDirectory.value / "target/checkstyle-output.xml"
  )
}

object CopyDependencies {
  import scala.sys.process.Process

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (Compile / crossTarget) { _ / "jars"}

  lazy val settings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory() && !dest.mkdirs()) {
        throw new IOException("Failed to create jars directory.")
      }

      // For the SparkConnect build, we manually call the assembly target to
      // produce the shaded Jar which happens automatically in the case of Maven.
      // Later, when the dependencies are copied, we manually copy the shaded Jar only.
      val fid = (LocalProject("connect") / assembly).value
      val fidClient = (LocalProject("connect-client-jvm") / assembly).value
      val fidProtobuf = (LocalProject("protobuf") / assembly).value

      (Compile / dependencyClasspath).value.map(_.data)
        .filter { jar => jar.isFile() }
        // Do not copy the Spark Connect JAR as it is unshaded in the SBT build.
        .foreach { jar =>
          val destJar = new File(dest, jar.getName())
          if (destJar.isFile()) {
            destJar.delete()
          }

          if (jar.getName.contains("spark-connect-common") &&
            !SbtPomKeys.profiles.value.contains("noshade-connect")) {
            // Don't copy the spark connect common JAR as it is shaded in the spark connect.
          } else if (jar.getName.contains("connect-client-jvm")) {
            // Do not place Spark Connect client jars as it is not built-in.
          } else if (jar.getName.contains("spark-connect") &&
            !SbtPomKeys.profiles.value.contains("noshade-connect")) {
            Files.copy(fid.toPath, destJar.toPath)
          } else if (jar.getName.contains("spark-protobuf") &&
            !SbtPomKeys.profiles.value.contains("noshade-protobuf")) {
            Files.copy(fidProtobuf.toPath, destJar.toPath)
          } else {
            Files.copy(jar.toPath(), destJar.toPath())
          }
        }

      // Here we get the full classpathes required for Spark Connect client including
      // ammonite, and exclude all spark-* jars. After that, we add the shaded Spark
      // Connect client assembly manually.
      Def.taskDyn {
        if (moduleName.value.contains("assembly")) {
          Def.task {
            val replClasspathes = (LocalProject("connect-client-jvm") / Compile / dependencyClasspath)
              .value.map(_.data).filter(_.isFile())
            val scalaBinaryVer = SbtPomKeys.effectivePom.value.getProperties.get(
              "scala.binary.version").asInstanceOf[String]
            val sparkVer = SbtPomKeys.effectivePom.value.getProperties.get(
              "spark.version").asInstanceOf[String]
            val dest = destPath.value
            val destDir = new File(dest, "connect-repl").toPath
            Files.createDirectories(destDir)

            val sourceAssemblyJar = Paths.get(
              BuildCommons.sparkHome.getAbsolutePath, "connector", "connect", "client",
              "jvm", "target", s"scala-$scalaBinaryVer", s"spark-connect-client-jvm-assembly-$sparkVer.jar")
            val destAssemblyJar = Paths.get(destDir.toString, s"spark-connect-client-jvm-assembly-$sparkVer.jar")
            Files.copy(sourceAssemblyJar, destAssemblyJar, StandardCopyOption.REPLACE_EXISTING)

            replClasspathes.foreach { f =>
              val destFile = Paths.get(destDir.toString, f.getName)
              if (!f.getName.startsWith("spark-")) {
                Files.copy(f.toPath, destFile, StandardCopyOption.REPLACE_EXISTING)
              }
            }
          }.dependsOn(LocalProject("connect-client-jvm") / assembly)
        } else {
          Def.task {}
        }
      }.value
    },
    (Compile / packageBin / crossTarget) := destPath.value,
    (Compile / packageBin) := (Compile / packageBin).dependsOn(copyDeps).value
  )

}

object TestSettings {
  import BuildCommons._
  private val defaultExcludedTags = Seq("org.apache.spark.tags.ChromeUITest",
    "org.apache.spark.deploy.k8s.integrationtest.YuniKornTag",
    "org.apache.spark.internal.io.cloud.IntegrationTestSuite") ++
    (if (System.getProperty("os.name").startsWith("Mac OS X") &&
        System.getProperty("os.arch").equals("aarch64")) {
      Seq("org.apache.spark.tags.ExtendedLevelDBTest")
    } else Seq.empty)

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    // Setting SPARK_DIST_CLASSPATH is a simple way to make sure any child processes
    // launched by the tests have access to the correct test-time classpath.
    (Test / envVars) ++= Map(
      "SPARK_DIST_CLASSPATH" ->
        (Test / fullClasspath).value.files.map(_.getAbsolutePath)
          .mkString(File.pathSeparator).stripSuffix(File.pathSeparator),
      "SPARK_PREPEND_CLASSES" -> "1",
      "SPARK_SCALA_VERSION" -> scalaBinaryVersion.value,
      "SPARK_TESTING" -> "1",
      "JAVA_HOME" -> sys.env.get("JAVA_HOME").getOrElse(sys.props("java.home")),
      "SPARK_BEELINE_OPTS" -> "-DmyKey=yourValue"),

    // Copy system properties to forked JVMs so that tests know proxy settings
    (Test / javaOptions) ++= {
      val q = "\""
      sys.props.toList
        .filter {
          case (key, value) => key.startsWith("http.") || key.startsWith("https.")
        }
        .map {
          case (key, value) => s"-D$key=$q$value$q"
        }
    },

    (Test / javaOptions) += s"-Djava.io.tmpdir=$testTempDir",
    (Test / javaOptions) += "-Dspark.test.home=" + sparkHome,
    (Test / javaOptions) += "-Dspark.testing=1",
    (Test / javaOptions) += "-Dspark.port.maxRetries=100",
    (Test / javaOptions) += "-Dspark.master.rest.enabled=false",
    (Test / javaOptions) += "-Dspark.memory.debugFill=true",
    (Test / javaOptions) += "-Dspark.ui.enabled=false",
    (Test / javaOptions) += "-Dspark.ui.showConsoleProgress=false",
    (Test / javaOptions) += "-Dspark.unsafe.exceptionOnMemoryLeak=true",
    (Test / javaOptions) += "-Dhive.conf.validation=false",
    (Test / javaOptions) += "-Dsun.io.serialization.extendedDebugInfo=false",
    (Test / javaOptions) += "-Dderby.system.durability=test",
    (Test / javaOptions) ++= {
      if ("true".equals(System.getProperty("java.net.preferIPv6Addresses"))) {
        Seq("-Djava.net.preferIPv6Addresses=true")
      } else {
        Seq.empty
      }
    },
    (Test / javaOptions) ++= System.getProperties.asScala.filter(_._1.startsWith("spark"))
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    (Test / javaOptions) += "-ea",
    (Test / javaOptions) ++= {
      val metaspaceSize = sys.env.get("METASPACE_SIZE").getOrElse("1300m")
      val heapSize = sys.env.get("HEAP_SIZE").getOrElse("4g")
      val extraTestJavaArgs = Array("-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Djdk.reflect.useDirectMethodHandle=false",
        "-Dio.netty.tryReflectionSetAccessible=true").mkString(" ")
      s"-Xmx$heapSize -Xss4m -XX:MaxMetaspaceSize=$metaspaceSize -XX:ReservedCodeCacheSize=128m -Dfile.encoding=UTF-8 $extraTestJavaArgs"
        .split(" ").toSeq
    },
    javaOptions ++= {
      val metaspaceSize = sys.env.get("METASPACE_SIZE").getOrElse("1300m")
      val heapSize = sys.env.get("HEAP_SIZE").getOrElse("4g")
      s"-Xmx$heapSize -XX:MaxMetaspaceSize=$metaspaceSize".split(" ").toSeq
    },
    (Test / javaOptions) ++= {
      val jdwpEnabled = sys.props.getOrElse("test.jdwp.enabled", "false").toBoolean

      if (jdwpEnabled) {
        val jdwpAddr = sys.props.getOrElse("test.jdwp.address", "localhost:0")
        val jdwpServer = sys.props.getOrElse("test.jdwp.server", "y")
        val jdwpSuspend = sys.props.getOrElse("test.jdwp.suspend", "y")
        ("-agentlib:jdwp=transport=dt_socket," +
          s"suspend=$jdwpSuspend,server=$jdwpServer,address=$jdwpAddr").split(" ").toSeq
      } else {
        Seq.empty
      }
    },
    // Exclude tags defined in a system property
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.exclude.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-l", tag) }.toSeq
      }.getOrElse(Nil): _*),
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.default.exclude.tags").map(tags => tags.split(",").toSeq)
        .map(tags => tags.filter(!_.trim.isEmpty)).getOrElse(defaultExcludedTags)
        .flatMap(tag => Seq("-l", tag)): _*),
    (Test / testOptions) += Tests.Argument(TestFrameworks.JUnit,
      sys.props.get("test.exclude.tags").map { tags =>
        Seq("--exclude-categories=" + tags)
      }.getOrElse(Nil): _*),
    // Include tags defined in a system property
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.include.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-n", tag) }.toSeq
      }.getOrElse(Nil): _*),
    (Test / testOptions) += Tests.Argument(TestFrameworks.JUnit,
      sys.props.get("test.include.tags").map { tags =>
        Seq("--include-categories=" + tags)
      }.getOrElse(Nil): _*),
    // Show full stack trace and duration in test cases.
    (Test / testOptions) += Tests.Argument("-oDF"),
    // Slowpoke notifications: receive notifications every 5 minute of tests that have been running
    // longer than two minutes.
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest, "-W", "120", "300"),
    (Test / testOptions) += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.github.sbt.junit" % "jupiter-interface" % "0.13.3" % "test",
    // `parallelExecutionInTest` controls whether test suites belonging to the same SBT project
    // can run in parallel with one another. It does NOT control whether tests execute in parallel
    // within the same JVM (which is controlled by `testForkedParallel`) or whether test cases
    // within the same suite can run in parallel (which is a ScalaTest runner option which is passed
    // to the underlying runner but is not a SBT-level configuration). This needs to be `true` in
    // order for the extra parallelism enabled by `SparkParallelTestGrouping` to take effect.
    // The `SERIAL_SBT_TESTS` check is here so the extra parallelism can be feature-flagged.
    (Test / parallelExecution) := { if (sys.env.contains("SERIAL_SBT_TESTS")) false else true },
    // Make sure the test temp directory exists.
    (Test / resourceGenerators) += Def.macroValueI((Test / resourceManaged) map { outDir: File =>
      var dir = new File(testTempDir)
      if (!dir.isDirectory()) {
        // Because File.mkdirs() can fail if multiple callers are trying to create the same
        // parent directory, this code tries to create parents one at a time, and avoids
        // failures when the directories have been created by somebody else.
        val stack = new ListBuffer[File]()
        while (!dir.isDirectory()) {
          stack.prepend(dir)
          dir = dir.getParentFile()
        }

        while (stack.nonEmpty) {
          val d = stack.remove(0)
          require(d.mkdir() || d.isDirectory(), s"Failed to create directory $d")
        }
      }
      Seq.empty[File]
    }).value,
    (Global / concurrentRestrictions) := {
      // The number of concurrent test groups is empirically chosen based on experience
      // with Jenkins flakiness.
      if (sys.env.contains("SERIAL_SBT_TESTS")) (Global / concurrentRestrictions).value
      else Seq(Tags.limit(Tags.ForkedTestGroup, 4))
    }
  )

}
