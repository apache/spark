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
import java.nio.file.Files
import java.util.Locale

import scala.io.Source
import scala.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import sbt._
import sbt.Classpaths.publishTask
import sbt.Keys._
import sbt.librarymanagement.{ VersionNumber, SemanticSelector }
import com.etsy.sbt.checkstyle.CheckstylePlugin.autoImport._
import com.simplytyped.Antlr4Plugin._
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}
import com.typesafe.tools.mima.plugin.MimaKeys
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks
import sbtassembly.AssemblyPlugin.autoImport._

import spray.revolver.RevolverPlugin._

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val sqlProjects@Seq(catalyst, sql, hive, hiveThriftServer, tokenProviderKafka010, sqlKafka010, avro) = Seq(
    "catalyst", "sql", "hive", "hive-thriftserver", "token-provider-kafka-0-10", "sql-kafka-0-10", "avro"
  ).map(ProjectRef(buildLocation, _))

  val streamingProjects@Seq(streaming, streamingKafka010) =
    Seq("streaming", "streaming-kafka-0-10").map(ProjectRef(buildLocation, _))

  val allProjects@Seq(
    core, graphx, mllib, mllibLocal, repl, networkCommon, networkShuffle, launcher, unsafe, tags, sketch, kvstore, _*
  ) = Seq(
    "core", "graphx", "mllib", "mllib-local", "repl", "network-common", "network-shuffle", "launcher", "unsafe",
    "tags", "sketch", "kvstore"
  ).map(ProjectRef(buildLocation, _)) ++ sqlProjects ++ streamingProjects

  val optionallyEnabledProjects@Seq(kubernetes, mesos, yarn,
    sparkGangliaLgpl, streamingKinesisAsl,
    dockerIntegrationTests, hadoopCloud, kubernetesIntegrationTests) =
    Seq("kubernetes", "mesos", "yarn",
      "ganglia-lgpl", "streaming-kinesis-asl",
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
    var contents = Source.fromFile(in).getLines.mkString("\n")
    for ((k, v) <- replacements) {
      require(contents.contains(k), s"Could not rewrite '$k' in original scalastyle config.")
      contents = contents.replace(k, v)
    }
    new PrintWriter(out) {
      write(contents)
      close()
    }
    out
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

  // Silencer: Scala compiler plugin for warning suppression
  // Aim: enable fatal warnings, but suppress ones related to using of deprecated APIs
  // depends on scala version:
  // <2.13.2 - silencer 1.7.5 and compiler settings to enable fatal warnings
  // 2.13.2+ - no silencer and configured warnings to achieve the same
  lazy val compilerWarningSettings: Seq[sbt.Def.Setting[_]] = Seq(
    libraryDependencies ++= {
      if (VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("<2.13.2"))) {
        val silencerVersion = "1.7.6"
        Seq(
          "org.scala-lang.modules" %% "scala-collection-compat" % "2.2.0",
          compilerPlugin("com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full),
          "com.github.ghik" % "silencer-lib" % silencerVersion % Provided cross CrossVersion.full
        )
      } else {
        Seq.empty
      }
    },
    (Compile / scalacOptions) ++= {
      if (VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("<2.13.2"))) {
        Seq(
          "-Xfatal-warnings",
          "-deprecation",
          "-Ywarn-unused-import",
          "-P:silencer:globalFilters=.*deprecated.*" //regex to catch deprecation warnings and suppress them
        )
      } else {
        Seq(
          // replace -Xfatal-warnings with fine-grained configuration, since 2.13.2
          // verbose warning on deprecation, error on all others
          // see `scalac -Wconf:help` for details
          "-Wconf:cat=deprecation:wv,any:e",
          // 2.13-specific warning hits to be muted (as narrowly as possible) and addressed separately
          // TODO(SPARK-33499): Enable this option when Scala 2.12 is no longer supported.
          // "-Wunused:imports",
          "-Wconf:cat=lint-multiarg-infix:wv",
          "-Wconf:cat=other-nullary-override:wv",
          "-Wconf:cat=other-match-analysis&site=org.apache.spark.sql.catalyst.catalog.SessionCatalog.lookupFunction.catalogFunction:wv",
          "-Wconf:cat=other-pure-statement&site=org.apache.spark.streaming.util.FileBasedWriteAheadLog.readAll.readFile:wv",
          "-Wconf:cat=other-pure-statement&site=org.apache.spark.scheduler.OutputCommitCoordinatorSuite.<local OutputCommitCoordinatorSuite>.futureAction:wv",
          "-Wconf:cat=other-pure-statement&site=org.apache.spark.sql.streaming.sources.StreamingDataSourceV2Suite.testPositiveCase.\\$anonfun:wv",
          // SPARK-33775 Suppress compilation warnings that contain the following contents.
          // TODO(SPARK-33805): Undo the corresponding deprecated usage suppression rule after
          //  fixed.
          "-Wconf:msg=^(?=.*?method|value|type|object|trait|inheritance)(?=.*?deprecated)(?=.*?since 2.13).+$:s",
          "-Wconf:msg=^(?=.*?Widening conversion from)(?=.*?is deprecated because it loses precision).+$:s",
          "-Wconf:msg=Auto-application to \\`\\(\\)\\` is deprecated:s",
          "-Wconf:msg=method with a single empty parameter list overrides method without any parameter list:s",
          "-Wconf:msg=method without a parameter list overrides a method with a single empty one:s",
          // SPARK-35574 Prevent the recurrence of compilation warnings related to `procedure syntax is deprecated`
          "-Wconf:cat=deprecation&msg=procedure syntax is deprecated:e",
          // SPARK-35496 Upgrade Scala to 2.13.7 and suppress:
          // 1. `The outer reference in this type test cannot be checked at run time`
          // 2. `the type test for pattern TypeA cannot be checked at runtime because it
          //    has type parameters eliminated by erasure`
          // 3. `abstract type TypeA in type pattern Seq[TypeA] (the underlying of
          //    Seq[TypeA]) is unchecked since it is eliminated by erasure`
          // 4. `fruitless type test: a value of TypeA cannot also be a TypeB`
          "-Wconf:cat=unchecked&msg=outer reference:s",
          "-Wconf:cat=unchecked&msg=eliminated by erasure:s",
          "-Wconf:msg=^(?=.*?a value of type)(?=.*?cannot also be).+$:s"
        )
      }
    }
  )

  lazy val sharedSettings = sparkGenjavadocSettings ++
                            compilerWarningSettings ++
      (if (sys.env.contains("NOLINT_ON_COMPILE")) Nil else enableScalaStyle) ++ Seq(
    (Compile / exportJars) := true,
    (Test / exportJars) := false,
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home").map { p => new File(p).getParentFile().getAbsolutePath() })
      .map(file),
    publishMavenStyle := true,
    unidocGenjavadocVersion := "0.18",

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
    (MavenCompile / publishLocal) := publishTask((MavenCompile / publishLocalConfiguration)).value,
    (SbtCompile / publishLocal) := publishTask((SbtCompile / publishLocalConfiguration)).value,
    publishLocal := Seq((MavenCompile / publishLocal), (SbtCompile / publishLocal)).dependOn.value,

    javaOptions ++= {
      val versionParts = System.getProperty("java.version").split("[+.\\-]+", 3)
      var major = versionParts(0).toInt
      if (major >= 16) Seq("--add-modules=jdk.incubator.vector,jdk.incubator.foreign", "-Dforeign.restricted=warn") else Seq.empty
    },

    (Compile / doc / javacOptions) ++= {
      val versionParts = System.getProperty("java.version").split("[+.\\-]+", 3)
      var major = versionParts(0).toInt
      if (major == 1) major = versionParts(1).toInt
      if (major >= 8) Seq("-Xdoclint:all", "-Xdoclint:-missing") else Seq.empty
    },

    javaVersion := SbtPomKeys.effectivePom.value.getProperties.get("java.version").asInstanceOf[String],

    (Compile / javacOptions) ++= Seq(
      "-encoding", UTF_8.name(),
      "-source", javaVersion.value
    ),
    // This -target and Xlint:unchecked options cannot be set in the Compile configuration scope since
    // `javadoc` doesn't play nicely with them; see https://github.com/sbt/sbt/issues/355#issuecomment-3817629
    // for additional discussion and explanation.
    (Compile / compile / javacOptions) ++= Seq(
      "-target", javaVersion.value,
      "-Xlint:unchecked"
    ),

    (Compile / scalacOptions) ++= Seq(
      s"-target:jvm-${javaVersion.value}",
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
        "org.apache.spark.util.collection"
      ).mkString(":"),
      "-doc-title", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    ) ++ {
      // Do not attempt to scaladoc javadoc comments under 2.12 since it can't handle inner classes
      if (scalaBinaryVersion.value == "2.12") Seq("-no-java-comments") else Seq.empty
    },

    // disable Mima check for all modules,
    // to be enabled in specific ones that have previous artifacts
    MimaKeys.mimaFailOnNoPrevious := false,

    // To prevent intermittent compilation failures, see also SPARK-33297
    // Apparently we can remove this when we use JDK 11.
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ optionallyEnabledProjects ++ assemblyProjects ++ copyJarsProjects ++ Seq(spark, tools))
    .foreach(enable(sharedSettings ++ DependencyOverrides.settings ++
      ExcludedDependencies.settings ++ Checkstyle.settings))

  /* Enable tests settings for all projects except examples, assembly and tools */
  (allProjects ++ optionallyEnabledProjects).foreach(enable(TestSettings.settings))

  val mimaProjects = allProjects.filterNot { x =>
    Seq(
      spark, hive, hiveThriftServer, catalyst, repl, networkCommon, networkShuffle, networkYarn,
      unsafe, tags, tokenProviderKafka010, sqlKafka010, kvstore, avro
    ).contains(x)
  }

  mimaProjects.foreach { x =>
    enable(MimaBuild.mimaSettings(sparkHome, x))(x)
  }

  /* Generate and pick the spark build info from extra-resources */
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
  enable(Unidoc.settings)(spark)

  /* Catalyst ANTLR generation settings */
  enable(Catalyst.settings)(catalyst)

  /* Spark SQL Core console settings */
  enable(SQL.settings)(sql)

  /* Hive console settings */
  enable(Hive.settings)(hive)

  // SPARK-14738 - Remove docker tests from main Spark build
  // enable(DockerIntegrationTests.settings)(dockerIntegrationTests)

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
    "org.apache.spark.sql.catalyst.expressions.DateExpressionsSuite",
    "org.apache.spark.sql.catalyst.expressions.HashExpressionsSuite",
    "org.apache.spark.sql.catalyst.expressions.CastSuite",
    "org.apache.spark.sql.catalyst.expressions.MathExpressionsSuite",
    "org.apache.spark.sql.hive.HiveExternalCatalogSuite",
    "org.apache.spark.sql.hive.StatisticsSuite",
    "org.apache.spark.sql.hive.client.VersionsSuite",
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
    "org.apache.spark.shuffle.KubernetesLocalDiskShuffleDataIOSuite"
  )

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

object Core {
  import scala.sys.process.Process
  lazy val settings = Seq(
    (Compile / resourceGenerators) += Def.task {
      val buildScript = baseDirectory.value + "/../build/spark-build-info"
      val targetDir = baseDirectory.value + "/target/extra-resources/"
      val command = Seq("bash", buildScript, targetDir, version.value)
      Process(command).!!
      val propsFile = baseDirectory.value / "target" / "extra-resources" / "spark-version-info.properties"
      Seq(propsFile)
    }.taskValue
  )
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
    dependencyOverrides += "com.google.guava" % "guava" % "18.0",
    resolvers += "DB2" at "https://app.camunda.com/nexus/content/repositories/public/",
    libraryDependencies += "com.oracle" % "ojdbc6" % "11.2.0.1.0" from "https://app.camunda.com/nexus/content/repositories/public/com/oracle/ojdbc6/11.2.0.1.0/ojdbc6-11.2.0.1.0.jar" // scalastyle:ignore
  )
}

/**
 * These settings run a hardcoded configuration of the Kubernetes integration tests using
 * minikube. Docker images will have the "dev" tag, and will be overwritten every time the
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
  val imageTag = settingKey[String]("Tag to use for images built during the test.")
  val namespace = settingKey[String]("Namespace where to run pods.")

  // Hack: this variable is used to control whether to build docker images. It's updated by
  // the tasks below in a non-obvious way, so that you get the functionality described in
  // the scaladoc above.
  private var shouldBuildImage = true

  lazy val settings = Seq(
    imageTag := "dev",
    namespace := "default",
    dockerBuild := {
      if (shouldBuildImage) {
        val dockerTool = s"$sparkHome/bin/docker-image-tool.sh"
        val bindingsDir = s"$sparkHome/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings"
        val cmd = Seq(dockerTool, "-m",
          "-t", imageTag.value,
          "-p", s"$bindingsDir/python/Dockerfile",
          "-R", s"$bindingsDir/R/Dockerfile",
          "-b", s"java_image_tag=${sys.env.getOrElse("JAVA_IMAGE_TAG", "8-jre-slim")}",
          "build"
        )
        val ec = Process(cmd).!
        if (ec != 0) {
          throw new IllegalStateException(s"Process '${cmd.mkString(" ")}' exited with $ec.")
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
      "-Dspark.kubernetes.test.deployMode=minikube",
      s"-Dspark.kubernetes.test.imageTag=${imageTag.value}",
      s"-Dspark.kubernetes.test.namespace=${namespace.value}",
      s"-Dspark.kubernetes.test.unpackSparkDir=$sparkHome"
    ),
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
  lazy val guavaVersion = sys.props.get("guava.version").getOrElse("14.0.1")
  lazy val settings = Seq(
    dependencyOverrides += "com.google.guava" % "guava" % guavaVersion,
    dependencyOverrides += "xerces" % "xercesImpl" % "2.12.0",
    dependencyOverrides += "jline" % "jline" % "2.14.6",
    dependencyOverrides += "org.apache.avro" % "avro" % "1.10.2")
}

/**
 * This excludes library dependencies in sbt, which are specified in maven but are
 * not needed by sbt build.
 */
object ExcludedDependencies {
  lazy val settings = Seq(
    libraryDependencies ~= { libs => libs.filterNot(_.name == "groovy-all") },
    // SPARK-33705: Due to sbt compiler issues, it brings exclusions defined in maven pom back to
    // the classpath directly and assemble test scope artifacts to assembly/target/scala-xx/jars,
    // which is also will be added to the classpath of some unit tests that will build a subprocess
    // to run `spark-submit`, e.g. HiveThriftServer2Test.
    //
    // These artifacts are for the jersey-1 API but Spark use jersey-2 ones, so it cause test
    // flakiness w/ jar conflicts issues.
    //
    // Also jersey-1 is only used by yarn module(see resource-managers/yarn/pom.xml) for testing
    // purpose only. Here we exclude them from the whole project scope and add them w/ yarn only.
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "com.sun.jersey"),
      ExclusionRule("javax.servlet", "javax.servlet-api"),
      ExclusionRule("javax.ws.rs", "jsr311-api"),
      ExclusionRule("io.netty", "netty-handler"),
      ExclusionRule("io.netty", "netty-transport-native-epoll"))
  )
}

/**
 * Project to pull previous artifacts of Spark for generating Mima excludes.
 */
object OldDeps {

  lazy val project = Project("oldDeps", file("dev"))
    .settings(oldDepsSettings)
    .disablePlugins(com.typesafe.sbt.pom.PomReaderPlugin)

  lazy val allPreviousArtifactKeys = Def.settingDyn[Seq[Set[ModuleID]]] {
    SparkBuild.mimaProjects
      .map { project => (project / MimaKeys.mimaPreviousArtifacts) }
      .map(k => Def.setting(k.value))
      .join
  }

  def oldDepsSettings() = Defaults.coreDefaultSettings ++ Seq(
    name := "old-deps",
    libraryDependencies := allPreviousArtifactKeys.value.flatten
  )
}

object Catalyst {
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
  lazy val settings = Seq(
    (console / initialCommands) :=
      """
        |import org.apache.spark.SparkContext
        |import org.apache.spark.sql.SQLContext
        |import org.apache.spark.sql.catalyst.analysis._
        |import org.apache.spark.sql.catalyst.dsl._
        |import org.apache.spark.sql.catalyst.errors._
        |import org.apache.spark.sql.catalyst.expressions._
        |import org.apache.spark.sql.catalyst.plans.logical._
        |import org.apache.spark.sql.catalyst.rules._
        |import org.apache.spark.sql.catalyst.util._
        |import org.apache.spark.sql.execution
        |import org.apache.spark.sql.functions._
        |import org.apache.spark.sql.types._
        |
        |val sc = new SparkContext("local[*]", "dev-shell")
        |val sqlContext = new SQLContext(sc)
        |import sqlContext.implicits._
        |import sqlContext._
      """.stripMargin,
    (console / cleanupCommands) := "sc.stop()"
  )
}

object Hive {

  lazy val settings = Seq(
    // Specially disable assertions since some Hive tests fail them
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_ == "-ea"),
    // Hive tests need higher metaspace size
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_.contains("MaxMetaspaceSize")),
    (Test / javaOptions) += "-XX:MaxMetaspaceSize=2g",
    // Supporting all SerDes requires us to depend on deprecated APIs, so we turn off the warnings
    // only for this subproject.
    scalacOptions := (scalacOptions map { currentOpts: Seq[String] =>
      currentOpts.filterNot(_ == "-deprecation")
    }).value,
    (console / initialCommands) :=
      """
        |import org.apache.spark.SparkContext
        |import org.apache.spark.sql.catalyst.analysis._
        |import org.apache.spark.sql.catalyst.dsl._
        |import org.apache.spark.sql.catalyst.errors._
        |import org.apache.spark.sql.catalyst.expressions._
        |import org.apache.spark.sql.catalyst.plans.logical._
        |import org.apache.spark.sql.catalyst.rules._
        |import org.apache.spark.sql.catalyst.util._
        |import org.apache.spark.sql.execution
        |import org.apache.spark.sql.functions._
        |import org.apache.spark.sql.hive._
        |import org.apache.spark.sql.hive.test.TestHive._
        |import org.apache.spark.sql.hive.test.TestHive.implicits._
        |import org.apache.spark.sql.types._""".stripMargin,
    (console / cleanupCommands) := "sparkContext.stop()",
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
    excludeDependencies --= Seq(
      ExclusionRule(organization = "com.sun.jersey"),
      ExclusionRule("javax.servlet", "javax.servlet-api"),
      ExclusionRule("javax.ws.rs", "jsr311-api")),
    Compile / unmanagedResources :=
      (Compile / unmanagedResources).value.filter(!_.getName.endsWith(s"$propFileName")),
    genConfigProperties := {
      val file = (Compile / classDirectory).value / s"org/apache/spark/deploy/yarn/$propFileName"
      val isHadoopProvided = SbtPomKeys.effectivePom.value.getProperties.get(hadoopProvidedProp)
      IO.write(file, s"$hadoopProvidedProp = $isHadoopProvided")
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
  import sbtassembly.AssemblyUtils._
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
      case "log4j.properties"                                  => MergeStrategy.discard
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

object Unidoc {

  import BuildCommons._
  import sbtunidoc.BaseUnidocPlugin
  import sbtunidoc.JavaUnidocPlugin
  import sbtunidoc.ScalaUnidocPlugin
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import sbtunidoc.JavaUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport._

  private def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
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
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/util/kvstore")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/catalyst")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/execution")))
      .map(_.filterNot(_.getCanonicalPath.contains("org/apache/spark/sql/internal")))
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
  }

  val unidocSourceBase = settingKey[String]("Base URL of source links in Scaladoc.")

  lazy val settings = BaseUnidocPlugin.projectSettings ++
                      ScalaUnidocPlugin.projectSettings ++
                      JavaUnidocPlugin.projectSettings ++
                      Seq (
    publish := {},

    (ScalaUnidoc / unidoc / unidocProjectFilter) :=
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, kubernetes,
        yarn, tags, streamingKafka010, sqlKafka010),
    (JavaUnidoc / unidoc / unidocProjectFilter) :=
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, kubernetes,
        yarn, tags, streamingKafka010, sqlKafka010),

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
      val versionParts = System.getProperty("java.version").split("[+.\\-]+", 3)
      var major = versionParts(0).toInt
      if (major == 1) major = versionParts(1).toInt

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
      ) ++ { if (major >= 9) Seq("--ignore-source-errors", "-notree") else Seq.empty }
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

object Checkstyle {
  lazy val settings = Seq(
    checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
    (Compile / checkstyle / javaSource) := baseDirectory.value / "src/main/java",
    (Test / checkstyle / javaSource) := baseDirectory.value / "src/test/java",
    checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
    checkstyleOutputFile := baseDirectory.value / "target/checkstyle-output.xml",
    (Test / checkstyleOutputFile) := baseDirectory.value / "target/checkstyle-output.xml"
  )
}

object CopyDependencies {

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (Compile / crossTarget) { _ / "jars"}

  lazy val settings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory() && !dest.mkdirs()) {
        throw new IOException("Failed to create jars directory.")
      }

      (Compile / dependencyClasspath).value.map(_.data)
        .filter { jar => jar.isFile() }
        .foreach { jar =>
          val destJar = new File(dest, jar.getName())
          if (destJar.isFile()) {
            destJar.delete()
          }
          Files.copy(jar.toPath(), destJar.toPath())
        }
    },
    (Compile / packageBin / crossTarget) := destPath.value,
    (Compile / packageBin) := (Compile / packageBin).dependsOn(copyDeps).value
  )

}

object TestSettings {
  import BuildCommons._
  private val defaultExcludedTags = Seq("org.apache.spark.tags.ChromeUITest")

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
      "JAVA_HOME" -> sys.env.get("JAVA_HOME").getOrElse(sys.props("java.home"))),
    (Test / javaOptions) += s"-Djava.io.tmpdir=$testTempDir",
    (Test / javaOptions) += "-Dspark.test.home=" + sparkHome,
    (Test / javaOptions) += "-Dspark.testing=1",
    (Test / javaOptions) += "-Dspark.port.maxRetries=100",
    (Test / javaOptions) += "-Dspark.master.rest.enabled=false",
    (Test / javaOptions) += "-Dspark.memory.debugFill=true",
    (Test / javaOptions) += "-Dspark.ui.enabled=false",
    (Test / javaOptions) += "-Dspark.ui.showConsoleProgress=false",
    (Test / javaOptions) += "-Dspark.unsafe.exceptionOnMemoryLeak=true",
    (Test / javaOptions) += "-Dspark.hadoop.hadoop.security.key.provider.path=test:///",
    (Test / javaOptions) += "-Dsun.io.serialization.extendedDebugInfo=false",
    (Test / javaOptions) += "-Dderby.system.durability=test",
    (Test / javaOptions) += "-Dio.netty.tryReflectionSetAccessible=true",
    (Test / javaOptions) ++= System.getProperties.asScala.filter(_._1.startsWith("spark"))
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    (Test / javaOptions) += "-ea",
    (Test / javaOptions) ++= {
      val metaspaceSize = sys.env.get("METASPACE_SIZE").getOrElse("1300m")
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
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        // SPARK-37070 In order to enable the UTs in `mllib-local` and `mllib` to use `mockito`
        // to mock `j.u.Random`, "-add-exports=java.base/jdk.internal.util.random=ALL-UNNAMED"
        // is added. Should remove it when `mockito` can mock `j.u.Random` directly.
        "--add-exports=java.base/jdk.internal.util.random=ALL-UNNAMED").mkString(" ")
      s"-Xmx4g -Xss4m -XX:MaxMetaspaceSize=$metaspaceSize -XX:ReservedCodeCacheSize=128m $extraTestJavaArgs"
        .split(" ").toSeq
    },
    javaOptions ++= {
      val metaspaceSize = sys.env.get("METASPACE_SIZE").getOrElse("1300m")
      s"-Xmx4g -XX:MaxMetaspaceSize=$metaspaceSize".split(" ").toSeq
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
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
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
