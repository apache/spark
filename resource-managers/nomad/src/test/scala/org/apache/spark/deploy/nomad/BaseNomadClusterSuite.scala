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

package org.apache.spark.deploy.nomad

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import com.google.common.io.Files
import org.apache.commons.lang3.SerializationUtils
import org.apache.http.HttpHost
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.launcher._
import org.apache.spark.launcher.SparkAppHandle.State
import org.apache.spark.launcher.SparkAppHandle.State.FINISHED
import org.apache.spark.scheduler.cluster.nomad.{SparkNomadJob, TestApplication}
import org.apache.spark.util.Utils

abstract class BaseNomadClusterSuite extends SparkFunSuite with BeforeAndAfterEach with Matchers {

  private val moduleDir = {
    val relativeToProjectRoot = new File(new File("resource-managers"), "nomad")
    if (relativeToProjectRoot.exists()) relativeToProjectRoot
    else new File("")
  }.getAbsoluteFile

  private val projectDir = moduleDir.getParentFile.getParentFile

  /**
   * The spark.nomad.test.host system property can is useful when the test machine is separated from
   * the Nomad cluster by NAT.
   */
  val hostnameAsSeenFromNomad: Option[String] = sys.props.get("spark.nomad.test.host")

  /**
   * An external Nomad server HTTP URL to test against.
   * When not specified, NOMAD_ADDR or the default (http://localhost/4646) will be used.
   */
  val nomadTestAddress: Option[HttpHost] =
    sys.props.get("spark.nomad.test.url")
      .map(HttpHost.create)

  /**
   * The docker image to use to run Spark in Nomad tasks.
   * When not specified, the `exec` driver is used instead.
   */
  val dockerImage: Option[String] = sys.props.get("spark.nomad.test.dockerImage")

  /**
   * The Spark distribution to use in Nomad tasks.
   * When not specified, the test looks for a distribution in the working directory
   * and serves it over HTTP.
   */
  val sparkDistribution: String = sys.props.get("spark.nomad.test.sparkDistribution").getOrElse {
    uniqueFileMatchingPattern(projectDir, "spark-.+?-bin-.+?\\.tgz")
      .getOrElse(sys.error("spark.nomad.test.sparkDistribution not set and can't find file in " +
        projectDir + " matching spark-.+?-bin-.+?\\.tgz"))
      .toString
  }

  private var oldSystemProperties: Properties = _
  protected var tempDir: File = _
  private var logConfDir: File = _

  override def beforeAll() {
    super.beforeAll()

    oldSystemProperties = SerializationUtils.clone(System.getProperties)

    tempDir = Utils.createTempDir()

    // log4j configuration for the processes spawned by the tests,
    // so that their output doesn't overwrite unit-tests.log.
    logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()
    Files.write(
      """log4j.rootCategory=DEBUG, console
        |log4j.appender.console=org.apache.log4j.ConsoleAppender
        |log4j.appender.console.target=System.err
        |log4j.appender.console.layout=org.apache.log4j.PatternLayout
        |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
        |log4j.logger.org.apache.hadoop=WARN
        |log4j.logger.org.apache.http=WARN
        |log4j.logger.org.apache.http.wire=ERROR
        |log4j.logger.org.eclipse.jetty=WARN
        |log4j.logger.org.mortbay=WARN
        |log4j.logger.org.spark_project.jetty=WARN
        |""".stripMargin,
      new File(logConfDir, "log4j.properties"),
      UTF_8
    )
  }

  override def afterAll() {
    System.setProperties(oldSystemProperties)
    super.afterAll()
  }


  var httpServer: HttpTestServer = _

  override protected def beforeEach(): Unit = {
    httpServer = HttpTestServer(hostnameAsSeenFromNomad)
  }

  override protected def afterEach(): Unit = {
    httpServer.close()
  }


  sealed abstract class DeployMode(override val toString: String)

  case object ClientMode extends DeployMode("client")

  case object ClusterMode extends DeployMode("cluster")

  sealed trait MainArtifact

  case class JarWithMainClass(jar: File, mainClass: String) extends MainArtifact

  case class FileResource(file: File) extends MainArtifact

  def mainClassName[A](mainClass: Class[A]): String = {
    assert(mainClass.getDeclaredMethod("main", classOf[Array[String]]) != null)
    mainClass.getName.stripSuffix("$")
  }

  lazy val testAppJar = {
    val jarNamePattern = "spark-nomad-test-apps_.*(?<!tests|sources|javadoc)\\.jar"
    val targetDir = new File(new File(moduleDir, "test-apps"), "target")
    uniqueFileMatchingPattern(targetDir, jarNamePattern).getOrElse(
      uniqueFileMatchingPattern(new File(targetDir, "scala-2.11"), jarNamePattern).getOrElse(
        sys.error("Can't find spark-nomad-test-apps jar")
      )
    )
  }

  def nomadTestApp[A <: TestApplication : ClassTag]: JarWithMainClass = {
    val klass = scala.reflect.classTag[A].runtimeClass
    assert(klass.getPackage == classOf[TestApplication].getPackage)
    JarWithMainClass(testAppJar, mainClassName(klass))
  }

  def createFile(prefix: String, suffix: String, contents: String): File = {
    val file = File.createTempFile(prefix, suffix, tempDir)
    Files.write(contents, file, UTF_8)
    file
  }

  def resource(deployMode: DeployMode, file: File): String = {
    deployMode match {
      case ClientMode => file.getAbsolutePath
      case ClusterMode => httpServer.addMapping("/some/path/" + file.getName, file)
    }
  }

  def runSpark(
      deployMode: DeployMode,
      mainArtifact: MainArtifact,
      appArgs: Seq[String] = Nil,
      sparkArgs: Seq[(String, String)] = Nil,
      extraClassPath: Seq[String] = Nil,
      extraJars: Seq[File] = Nil,
      extraFiles: Seq[File] = Nil,
      extraConf: Map[String, String] = Map(),
      extraEnv: Map[String, String] = Map()): State = {

    val propsFile = createConfFile(extraClassPath = extraClassPath, extraConf = extraConf)

    val launcher = new SparkLauncher(extraEnv.asJava)
    mainArtifact match {
      case JarWithMainClass(jar, klass) =>
        launcher.setMainClass(klass)
        launcher.setAppResource(resource(deployMode, jar))
      case FileResource(file) =>
        launcher.setAppResource(resource(deployMode, file))
    }
    launcher.setSparkHome(sys.props("spark.test.home"))
      .setMaster(nomadTestAddress.fold("nomad")("nomad:" +))
      .setDeployMode(deployMode.toString)
      .setConf("spark.executor.instances", "1")
      .setPropertiesFile(propsFile)
      .addSparkArg("--verbose")
      .addSparkArg("--driver-memory", "600m")
      .addSparkArg("--executor-memory", "500m")
      .addAppArgs(appArgs.toArray: _*)

    if (deployMode == ClientMode) hostnameAsSeenFromNomad.foreach { hostname =>
      launcher.setConf("spark.driver.host", hostname)
      launcher.setConf("spark.driver.bindAddress", "0.0.0.0")
    }

    sparkArgs.foreach { case (name, value) =>
      if (value != null) {
        launcher.addSparkArg(name, value)
      } else {
        launcher.addSparkArg(name)
      }
    }
    extraJars.foreach(jar => launcher.addJar(resource(deployMode, jar)))
    extraFiles.foreach(file => launcher.addFile(resource(deployMode, file)))

    launcher.setVerbose(true)

    val handle = launcher.startApplication()
    try {
      eventually(timeout(3 minutes), interval(1 second)) {
        val state = handle.getState()
        logDebug("CURRENT STATE IS " + state)
        assert(state.isFinal())
      }
    } finally {
      handle.kill()
    }

    handle.getState()
  }

  def checkResult(
      finalState: State,
      expectedPathValues: (String, String)*): Unit = {
    val expectedPathValueOptions =
      expectedPathValues.map {
        case (path, expectedValue) => (path, Some(expectedValue))
      }
    val actualServerContents = expectedPathValues.map {
      case (path, expectedValue) => (path, httpServer.valuePutToPath(path))
    }
    (finalState, actualServerContents) should be(FINISHED, expectedPathValueOptions)
  }

  def createConfFile(
      extraClassPath: Seq[String] = Nil,
      extraConf: Map[String, String] = Map()): String = {
    val props = new Properties()

    dockerImage.foreach(props.put(SparkNomadJob.DOCKER_IMAGE.key, _))

    props.put(SparkNomadJob.SPARK_DISTRIBUTION.key,
      if (sparkDistribution.indexOf(":") == -1) {
        val file = new File(sparkDistribution)
        httpServer.addMapping("/" + file.getName, file)
      } else {
        sparkDistribution
      }
    )

    val testClasspath =
      new TestClasspathBuilder().buildClassPath(
        logConfDir.getAbsolutePath +
          File.pathSeparator +
          extraClassPath.mkString(File.pathSeparator)
      )
        .asScala
        .mkString(File.pathSeparator)

    props.put("spark.driver.extraClassPath", testClasspath)
    props.put("spark.executor.extraClassPath", testClasspath)

    // SPARK-4267: make sure java options are propagated correctly.
    props.setProperty("spark.driver.extraJavaOptions", "-Dfoo=\"one two three\"")
    props.setProperty("spark.executor.extraJavaOptions", "-Dfoo=\"one two three\"")

    sys.props.foreach { case (k, v) =>
      if (k.startsWith("spark.")) {
        props.setProperty(k, v)
      }
    }
    extraConf.foreach { case (k, v) => props.setProperty(k, v) }

    val propsFile = File.createTempFile("spark", ".properties", tempDir)
    val writer = new OutputStreamWriter(new FileOutputStream(propsFile), UTF_8)
    try props.store(writer, "Spark properties.")
    finally writer.close()
    propsFile.getAbsolutePath
  }

  private def uniqueFileMatchingPattern(directory: File, pattern: String): Option[File] = {
    directory.listFiles(
      new FilenameFilter {
        override def accept(dir: File, name: String): Boolean =
          name.matches(pattern)
      }
    ) match {
      case null => sys.error(s"Can't find directory $directory")
      case Array() => None
      case Array(file) => Some(file)
      case files => sys.error(
        files.mkString(
          s"Expected 1 file in $directory matching $pattern, found ${files.length}: ", ", ", "")
      )
    }
  }

}
