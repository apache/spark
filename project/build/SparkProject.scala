import sbt._
import sbt.Process._

import assembly._

import de.element34.sbteclipsify._


class SparkProject(info: ProjectInfo) extends ParentProject(info) with IdeaProject {

  lazy val core = project("core", "Spark Core", new CoreProject(_))

  lazy val examples = project("examples", "Spark Examples", new ExamplesProject(_), core)

  lazy val bagel = project("bagel", "Bagel", new BagelProject(_), core)

  trait BaseProject extends BasicScalaProject with ScalaPaths with Eclipsify with IdeaProject {
    override def compileOptions = super.compileOptions ++ Seq(Unchecked)
  }

  class CoreProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject with DepJar with XmlTestReport

  class ExamplesProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject

  class BagelProject(info: ProjectInfo) extends DefaultProject(info) with BaseProject with DepJar with XmlTestReport
	
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
}
