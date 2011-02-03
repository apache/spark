import sbt._
import sbt.Process._

import assembly._

import de.element34.sbteclipsify._


class SparkProject(info: ProjectInfo)
extends ParentProject(info) with IdeaProject
{
  lazy val core = project("core", "Spark Core", new CoreProject(_))

  lazy val examples =
    project("examples", "Spark Examples", new ExamplesProject(_), core)

  class CoreProject(info: ProjectInfo)
  extends DefaultProject(info) with Eclipsify with IdeaProject with AssemblyBuilder
  {
    def testReportDir = outputPath / "test-report"

    // Create an XML test report using ScalaTest's -u option. Unfortunately
    // there is currently no way to call this directly from SBT without
    // executing a subprocess.
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
      val process = Process(command, path("."), "JAVA_OPTS" -> "-Xmx500m")
      process !

      None
    }.dependsOn(compile, testCompile).describedAs("Generate XML test report.")

    def singleJarExclude(base: PathFinder) = {
      (base / "scala" ** "*") +++ ( // exclude scala library
        (base / "META-INF" ** "*") --- // generally ignore the hell out of META-INF
        (base / "META-INF" / "services" ** "*") --- // include all service providers
        (base / "META-INF" / "maven" ** "*")) // include all Maven POMs and such
    }

    def singleJarTempDir = outputPath / "single-jar-classes"

    def singleJarOutputPath =
      outputPath / (name.toLowerCase.replace(" ", "-") + "-single-jar-" + version.toString + ".jar")

    // Create a JAR with Spark Core and all its dependencies. We use some methods in
    // the AssemblyBuilder plugin, but because this plugin attempts to package Scala
    // too, we leave that out using our own exclude filter (singleJarExclude).
    lazy val singleJar = {
      packageTask(
        Path.lazyPathFinder(assemblyPaths(singleJarTempDir,
                                          assemblyClasspath,
                                          assemblyExtraJars,
                                          singleJarExclude)),
        singleJarOutputPath,
        packageOptions)
    }.dependsOn(compile).describedAs("Build a single JAR with project and its dependencies")
  }

  class ExamplesProject(info: ProjectInfo)
  extends DefaultProject(info) with Eclipsify with IdeaProject
  {
  }
}
