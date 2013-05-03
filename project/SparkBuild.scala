
import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import twirl.sbt.TwirlPlugin._
// For Sonatype publishing
//import com.jsuereth.pgp.sbtplugin.PgpKeys._

object SparkBuild extends Build {
  // Hadoop version to build against. For example, "0.20.2", "0.20.205.0", or
  // "1.0.4" for Apache releases, or "0.20.2-cdh3u5" for Cloudera Hadoop.
  val HADOOP_VERSION = "1.0.4"
  val HADOOP_MAJOR_VERSION = "1"
  val HADOOP_YARN = false

  // For Hadoop 2 versions such as "2.0.0-mr1-cdh4.1.1", set the HADOOP_MAJOR_VERSION to "2"
  //val HADOOP_VERSION = "2.0.0-mr1-cdh4.1.1"
  //val HADOOP_MAJOR_VERSION = "2"
  //val HADOOP_YARN = false

  // For Hadoop 2 YARN support
  //val HADOOP_VERSION = "2.0.2-alpha"
  //val HADOOP_MAJOR_VERSION = "2"
  //val HADOOP_YARN = true

  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, repl, examples, bagel, graph, streaming)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val repl = Project("repl", file("repl"), settings = replSettings) dependsOn (core)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (core) dependsOn (streaming)

  lazy val bagel = Project("bagel", file("bagel"), settings = bagelSettings) dependsOn (core)

  lazy val graph = Project("graph", file("graph"), settings = graphSettings) dependsOn (core, bagel)

  lazy val streaming = Project("streaming", file("streaming"), settings = streamingSettings) dependsOn (core)

  // A configuration to set an alternative publishLocalConfiguration
  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.spark-project",
    version := "0.8.0-SNAPSHOT",
    scalaVersion := "2.9.3",
    scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),

    // Fork new JVMs for tests and set Java options for those
    fork := true,
    javaOptions += "-Xmx2g",

    // Only allow one test at a time, even across projects, since they run in the same JVM
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

    // Shared between both core and streaming.
    resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/"),

    // For Sonatype publishing
    resolvers ++= Seq("sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "sonatype-staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"),

    publishMavenStyle := true,

    //useGpg in Global := true,

    pomExtra := (
      <url>http://spark-project.org/</url>
      <licenses>
        <license>
          <name>BSD License</name>
          <url>https://github.com/mesos/spark/blob/master/LICENSE</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <connection>scm:git:git@github.com:mesos/spark.git</connection>
        <url>scm:git:git@github.com:mesos/spark.git</url>
      </scm>
      <developers>
        <developer>
          <id>matei</id>
          <name>Matei Zaharia</name>
          <email>matei.zaharia@gmail.com</email>
          <url>http://www.cs.berkeley.edu/~matei</url>
          <organization>U.C. Berkeley Computer Science</organization>
          <organizationUrl>http://www.cs.berkeley.edu/</organizationUrl>
        </developer>
      </developers>
    ),

/*
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("sonatype-staging"  at nexus + "service/local/staging/deploy/maven2")
    },

*/

    libraryDependencies ++= Seq(
      "io.netty" % "netty" % "3.5.3.Final",
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "com.novocode" % "junit-interface" % "0.9" % "test",
      "org.easymock" % "easymock" % "3.1" % "test"
    ),
    /* Workaround for issue #206 (fixed after SBT 0.11.0) */
    watchTransitiveSources <<= Defaults.inDependencies[Task[Seq[File]]](watchSources.task,
      const(std.TaskExtra.constant(Nil)), aggregate = true, includeRoot = true) apply { _.join.map(_.flatten) },

    otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),
    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn
  )

  val slf4jVersion = "1.6.1"

  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")

  def coreSettings = sharedSettings ++ Seq(
    name := "spark-core",
    resolvers ++= Seq(
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Spray Repository" at "http://repo.spray.cc/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Twitter4J Repository" at "http://twitter4j.org/maven2/"
    ),

    libraryDependencies ++= Seq(
      "io.netty" % "netty" % "3.5.3.Final",
      "com.google.guava" % "guava" % "11.0.1",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-daemon" % "commons-daemon" % "1.0.10",
      "com.ning" % "compress-lzf" % "0.8.4",
      "asm" % "asm-all" % "3.3.1",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "de.javakaffee" % "kryo-serializers" % "0.22",
      "com.typesafe.akka" % "akka-actor" % "2.0.3" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-remote" % "2.0.3" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-slf4j" % "2.0.3" excludeAll(excludeNetty),
      "it.unimi.dsi" % "fastutil" % "6.4.4",
      "colt" % "colt" % "1.2.0",
      "cc.spray" % "spray-can" % "1.0-M2.1" excludeAll(excludeNetty),
      "cc.spray" % "spray-server" % "1.0-M2.1" excludeAll(excludeNetty),
      "cc.spray" % "spray-json_2.9.2" % "1.1.1" excludeAll(excludeNetty),
      "org.apache.mesos" % "mesos" % "0.9.0-incubating"
    ) ++ (
      if (HADOOP_MAJOR_VERSION == "2") {
        if (HADOOP_YARN) {
          Seq(
            // Exclude rule required for all ?
            "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty),
            "org.apache.hadoop" % "hadoop-yarn-api" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty),
            "org.apache.hadoop" % "hadoop-yarn-common" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty),
            "org.apache.hadoop" % "hadoop-yarn-client" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty)
          )
        } else {
          Seq(
            "org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty),
            "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty)
          )
        }
      } else {
        Seq("org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION excludeAll(excludeJackson, excludeNetty) )
      }),
    unmanagedSourceDirectories in Compile <+= baseDirectory{ _ /
      ( if (HADOOP_YARN && HADOOP_MAJOR_VERSION == "2") {
        "src/hadoop2-yarn/scala"
      } else {
        "src/hadoop" + HADOOP_MAJOR_VERSION + "/scala"
      } )
    }
  ) ++ assemblySettings ++ extraAssemblySettings ++ Twirl.settings

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def replSettings = sharedSettings ++ Seq(
    name := "spark-repl",
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
  )

  def examplesSettings = sharedSettings ++ Seq(
    name := "spark-examples",
    libraryDependencies ++= Seq("com.twitter" % "algebird-core_2.9.2" % "0.1.11")
  )

  def bagelSettings = sharedSettings ++ Seq(name := "spark-bagel")

  def graphSettings = sharedSettings ++ Seq(name := "spark-graph")

  def streamingSettings = sharedSettings ++ Seq(
    name := "spark-streaming",
    libraryDependencies ++= Seq(
      "org.apache.flume" % "flume-ng-sdk" % "1.2.0" % "compile" excludeAll(excludeNetty),
      "com.github.sgroschupf" % "zkclient" % "0.1",
      "org.twitter4j" % "twitter4j-stream" % "3.0.3" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-zeromq" % "2.0.3" excludeAll(excludeNetty)
    )
  ) ++ assemblySettings ++ extraAssemblySettings

  def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
