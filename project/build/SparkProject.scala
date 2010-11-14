import sbt._
import de.element34.sbteclipsify._

import Process._

class SparkProject(info: ProjectInfo)
extends DefaultProject(info) with Eclipsify
{
  val TARGET = path("target") / "scala_2.8.1"

  val NATIVE_DIR = path("src") / "main" / "native"

  val NATIVE_SOURCES = NATIVE_DIR * "*.c"

  val NATIVE_LIB = {
    if (System.getProperty("os.name") == "Mac OS X")
      "libspark_native.dylib"
    else
      "libspark_native.so"
  }

  lazy val native = fileTask(TARGET / NATIVE_LIB from NATIVE_SOURCES) {
    val makeTarget = " ../../../target/scala_2.8.1/native/" + NATIVE_LIB
    (("make -C " + NATIVE_DIR + " " + makeTarget) ! log)
    None
  } dependsOn(compile) describedAs("Compiles native library.")

  val TEST_REPORT_DIR = TARGET / "test-report"

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
  } dependsOn(compile, testCompile) describedAs("Generate XML test report.")
}
