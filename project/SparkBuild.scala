import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SparkBuild extends Build {

  lazy val root = Project("root", file("."), settings = sharedSettings) aggregate(core, repl, examples, bagel)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val repl = Project("repl", file("repl"), settings = replSettings) dependsOn (core)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (core)

  lazy val bagel = Project("bagel", file("bagel"), settings = bagelSettings) dependsOn (core)

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.spark-project",
    version := "0.4-SNAPSHOT",
    scalaVersion := "2.9.1",
    scalacOptions := Seq(/*"-deprecation",*/ "-unchecked", "-optimize"), // -deprecation is too noisy due to usage of old Hadoop API, enable it once that's no longer an issue
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),
    publishTo <<= baseDirectory { base => Some(Resolver.file("Local", base / "target" / "maven" asFile)(Patterns(true, Resolver.mavenStyleBasePattern))) },
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.5.3.v20111011",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test",
      "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"
    ),
    /* Workaround for issue #206 (fixed after SBT 0.11.0) */
    watchTransitiveSources <<= Defaults.inDependencies[Task[Seq[File]]](watchSources.task,
      const(std.TaskExtra.constant(Nil)), aggregate = true, includeRoot = true) apply { _.join.map(_.flatten) }
  )

  val slf4jVersion = "1.6.1"

  def coreSettings = sharedSettings ++ Seq(
    name := "spark-core",
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "r09",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "com.ning" % "compress-lzf" % "0.8.4",
      "org.apache.hadoop" % "hadoop-core" % "0.20.2",
      "asm" % "asm-all" % "3.3.1",
      "com.google.protobuf" % "protobuf-java" % "2.3.0",
      "de.javakaffee" % "kryo-serializers" % "0.9"
    )
  ) ++ assemblySettings ++ Seq(test in assembly := {})

  def replSettings = sharedSettings ++ Seq(
    name := "spark-repl",
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
  )

  def examplesSettings = sharedSettings ++ Seq(
    name := "spark-examples",
    libraryDependencies += "colt" % "colt" % "1.2.0"
  )

  def bagelSettings = sharedSettings ++ Seq(name := "spark-bagel")
}
