import sbt._
import de.element34.sbteclipsify._

import sbt.Process._

class SparkProject(info: ProjectInfo)
extends ParentProject(info) with IdeaProject
{
  lazy val core = project("core", "Spark Core", new CoreProject(_))

  lazy val examples =
    project("examples", "Spark Examples", new ExamplesProject(_), core)

  class CoreProject(info: ProjectInfo)
  extends DefaultProject(info) with Eclipsify with IdeaProject
  {
    val TARGET = path("target") / "scala_2.8.1"

    val TEST_REPORT_DIR = TARGET / "test-report"

    // Create an XML test report using ScalaTest's -u option. Unfortunately
    // there is currently no way to call this directly from SBT without
    // executing a subprocess.
    lazy val testReport = task {
      log.info("Creating " + TEST_REPORT_DIR + "...")
      if (!TEST_REPORT_DIR.exists) {
        TEST_REPORT_DIR.asFile.mkdirs()
      }

      log.info("Executing org.scalatest.tools.Runner...")
      val command = ("scala -classpath " + testClasspath.absString + 
                     " org.scalatest.tools.Runner -o " + 
                     " -u " + TEST_REPORT_DIR.absolutePath +
                     " -p " + (TARGET / "test-classes").absolutePath)
      val process = Process(command, path("."), "JAVA_OPTS" -> "-Xmx500m")
      process !

      None
    }.dependsOn(compile, testCompile).describedAs("Generate XML test report.")
  }

  class ExamplesProject(info: ProjectInfo)
  extends DefaultProject(info) with Eclipsify with IdeaProject
  {
  }
}
