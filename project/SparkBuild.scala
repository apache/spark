import sbt._
import Keys._

object SparkBuild extends Build {

  lazy val root = Project("root", file("."), settings = sharedSettings) aggregate(core, repl, examples, bagel)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val repl = Project("repl", file("repl"), settings = replSettings) dependsOn (core)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (core)

  lazy val bagel = Project("bagel", file("bagel"), settings = bagelSettings) dependsOn (core)

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.spark-project",
    version := "0.4-SNAPSHOT",
    scalaVersion := "2.9.0-1",
    scalacOptions := Seq(/*"-deprecation",*/ "-unchecked"), // TODO Enable -deprecation and fix all warnings
    unmanagedJars in Compile <<= baseDirectory map { base => (base ** "*.jar").classpath },
    retrieveManaged := true,
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.4.2.v20110526",
      "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test",
      "org.scala-tools.testing" % "scalacheck_2.9.0-1" % "1.9" % "test"
    )
  )

  val slf4jVersion = "1.6.1"

  //FIXME XmlTestReport
  def coreSettings = sharedSettings ++ Seq(libraryDependencies ++= Seq(
    "com.google.guava" % "guava" % "r09",
    "log4j" % "log4j" % "1.2.16",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "com.ning" % "compress-lzf" % "0.7.0",
    "org.apache.hadoop" % "hadoop-core" % "0.20.2",
    "asm" % "asm-all" % "3.3.1"
  )) ++ DepJarPlugin.depJarSettings

  //FIXME XmlTestReport
  def replSettings = sharedSettings ++
      Seq(libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)) ++
      DepJarPlugin.depJarSettings

  def examplesSettings = sharedSettings ++ Seq(libraryDependencies += "colt" % "colt" % "1.2.0")

  //FIXME XmlTestReport
  def bagelSettings = sharedSettings ++ DepJarPlugin.depJarSettings
}

// Project mixin for an XML-based ScalaTest report. Unfortunately
// there is currently no way to call this directly from SBT without
// executing a subprocess.
//trait XmlTestReport extends BasicScalaProject {
//  def testReportDir = outputPath / "test-report"
//
//  lazy val testReport = task {
//    log.info("Creating " + testReportDir + "...")
//    if (!testReportDir.exists) {
//      testReportDir.asFile.mkdirs()
//    }
//    log.info("Executing org.scalatest.tools.Runner...")
//    val command = ("scala -classpath " + testClasspath.absString +
//                   " org.scalatest.tools.Runner -o " +
//                   " -u " + testReportDir.absolutePath +
//                   " -p " + (outputPath / "test-classes").absolutePath)
//    Process(command, path("."), "JAVA_OPTS" -> "-Xmx500m") !
//
//    None
//  }.dependsOn(compile, testCompile).describedAs("Generate XML test report.")
//}
