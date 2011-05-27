import sbt._
import sbt.Process._

import assembly._

import de.element34.sbteclipsify._


class SparkProject(info: ProjectInfo) extends ParentProject(info) with IdeaProject {

  lazy val core = project("core", "Spark Core", new CoreProject(_))

//  lazy val repl = project("repl", "Spark REPL", new ReplProject(_), core)

  lazy val examples = project("examples", "Spark Examples", new ExamplesProject(_), core)

  lazy val bagel = project("bagel", "Bagel", new BagelProject(_), core)

  lazy val jettyWebapp = "org.eclipse.jetty" % "jetty-webapp" % "7.4.1.v20110513" % "provided"

  trait BaseProject extends BasicScalaProject with ScalaPaths with BasicPackagePaths with Eclipsify with IdeaProject {
    override def compileOptions = super.compileOptions ++ Seq(Unchecked)
    override def packageDocsJar = defaultJarPath("-javadoc.jar")
    override def packageSrcJar= defaultJarPath("-sources.jar")
    lazy val sourceArtifact = Artifact.sources(artifactID)
    lazy val docsArtifact = Artifact.javadoc(artifactID)
    override def packageToPublishActions = super.packageToPublishActions ++ Seq(packageDocs, packageSrc)
  }

  class CoreProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject with DepJar with XmlTestReport {
    val guava = "com.google.guava" % "guava" % "r09"
    val log4j = "log4j" % "log4j" % "1.2.16"
    val slf4jVersion = "1.6.1"
    val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion
    val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % slf4jVersion
    val compressLzf = "com.ning" % "compress-lzf" % "0.7.0"
    val hadoop = "org.apache.hadoop" % "hadoop-core" % "0.20.2"
    val asm = "asm" % "asm-all" % "3.3.1"
    val scalaTest = "org.scalatest" % "scalatest_2.9.0" % "1.4.1" % "test"
    val scalaCheck = "org.scala-tools.testing" % "scalacheck_2.9.0" % "1.8" % "test"
    val jetty = jettyWebapp
  }

  class ReplProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject with DepJar with XmlTestReport {
    val jetty = jettyWebapp
  }

  class ExamplesProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject {
    val colt = "colt" % "colt" % "1.2.0"
  }

  class BagelProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject with DepJar with XmlTestReport {
    val jetty = jettyWebapp
  }
	
}


// Project mixin for an XML-based ScalaTest report. Unfortunately
// there is currently no way to call this directly from SBT without
// executing a subprocess.
trait XmlTestReport extends BasicScalaProject {
  def testReportDir = outputPath / "test-report"

  lazy val testReport = task {
    log.info("Creating " + testReportDir + "...")
    if (!testReportDir.exists) {
      testReportDir.asFile.mkdirs()
    }
    log.info("Executing org.scalatest.tools.Runner...")
    val command = ("scala -classpath " + testClasspath.absString +
                   " org.scalatest.tools.Runner -o " +
                   " -u " + testReportDir.absolutePath +
                   " -p " + (outputPath / "test-classes").absolutePath)
    Process(command, path("."), "JAVA_OPTS" -> "-Xmx500m") !

    None
  }.dependsOn(compile, testCompile).describedAs("Generate XML test report.")
}


// Project mixin for creating a JAR with  a project's dependencies. This is based
// on the AssemblyBuilder plugin, but because this plugin attempts to package Scala
// and our project too, we leave that out using our own exclude filter (depJarExclude).
trait DepJar extends AssemblyBuilder {
  def depJarExclude(base: PathFinder) = {
    (base / "scala" ** "*") +++ // exclude scala library
    (base / "spark" ** "*") +++ // exclude Spark classes
    ((base / "META-INF" ** "*") --- // generally ignore the hell out of META-INF
     (base / "META-INF" / "services" ** "*") --- // include all service providers
     (base / "META-INF" / "maven" ** "*")) // include all Maven POMs and such
  }

  def depJarTempDir = outputPath / "dep-classes"

  def depJarOutputPath =
    outputPath / (name.toLowerCase.replace(" ", "-") + "-dep-" + version.toString + ".jar")

  lazy val depJar = {
    packageTask(
      Path.lazyPathFinder(assemblyPaths(depJarTempDir,
                                        assemblyClasspath,
                                        assemblyExtraJars,
                                        depJarExclude)),
      depJarOutputPath,
      packageOptions)
  }.dependsOn(compile).describedAs("Bundle project's dependencies into a JAR.")
  override def managedStyle = ManagedStyle.Maven
}
