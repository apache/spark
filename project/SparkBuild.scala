import sbt._
import Keys._
import sbtassembly.Plugin.{assemblySettings, Assembly}

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
      "org.eclipse.jetty" % "jetty-server" % "7.4.2.v20110526",
      "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test",
      "org.scala-tools.testing" % "scalacheck_2.9.0-1" % "1.9" % "test"
    )
  )

  val slf4jVersion = "1.6.1"

  def coreSettings = sharedSettings ++ Seq(libraryDependencies ++= Seq(
    "com.google.guava" % "guava" % "r09",
    "log4j" % "log4j" % "1.2.16",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "com.ning" % "compress-lzf" % "0.7.0",
    "org.apache.hadoop" % "hadoop-core" % "0.20.2",
    "asm" % "asm-all" % "3.3.1",
    "com.google.protobuf" % "protobuf-java" % "2.3.0",
    "de.javakaffee" % "kryo-serializers" % "0.9"
  )) ++ assemblySettings ++ Seq(test in Assembly := {})

  def replSettings = sharedSettings ++
      Seq(libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _))

  def examplesSettings = sharedSettings ++ Seq(libraryDependencies += "colt" % "colt" % "1.2.0")

  def bagelSettings = sharedSettings
}
