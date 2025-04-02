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

package org.apache.spark

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.net.{HttpURLConnection, InetSocketAddress, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles, Paths}
import java.nio.file.attribute.PosixFilePermission.{OWNER_EXECUTE, OWNER_READ, OWNER_WRITE}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.{EnumSet, Locale}
import java.util.concurrent.{TimeoutException, TimeUnit}
import java.util.jar.{JarEntry, JarOutputStream, Manifest}
import java.util.regex.Pattern
import javax.net.ssl._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.{classTag, ClassTag}
import scala.sys.process.Process
import scala.util.Try

import com.google.common.io.{ByteStreams, Files}
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.ConsoleAppender
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.{SparkTestUtils, Utils}

/**
 * Utilities for tests. Included in main codebase since it's used by multiple
 * projects.
 *
 * TODO: See if we can move this to the test codebase by specifying
 * test dependencies between projects.
 */
private[spark] object TestUtils extends SparkTestUtils {

  /**
   * Create a jar that defines classes with the given names.
   *
   * Note: if this is used during class loader tests, class names should be unique
   * in order to avoid interference between tests.
   */
  def createJarWithClasses(
      classNames: Seq[String],
      toStringValue: String = "",
      classNamesWithBase: Seq[(String, String)] = Seq.empty,
      classpathUrls: Seq[URL] = Seq.empty): URL = {
    val tempDir = Utils.createTempDir()
    val files1 = for (name <- classNames) yield {
      createCompiledClass(name, tempDir, toStringValue, classpathUrls = classpathUrls)
    }
    val files2 = for ((childName, baseName) <- classNamesWithBase) yield {
      createCompiledClass(childName, tempDir, toStringValue, baseName, classpathUrls)
    }
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    createJar(files1 ++ files2, jarFile)
  }

  /**
   * Create a jar file containing multiple files. The `files` map contains a mapping of
   * file names in the jar file to their contents.
   */
  def createJarWithFiles(files: Map[String, String], dir: File = null): URL = {
    val tempDir = Option(dir).getOrElse(Utils.createTempDir())
    val jarFile = File.createTempFile("testJar", ".jar", tempDir)
    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    files.foreach { case (k, v) =>
      val entry = new JarEntry(k)
      jarStream.putNextEntry(entry)
      ByteStreams.copy(new ByteArrayInputStream(v.getBytes(StandardCharsets.UTF_8)), jarStream)
    }
    jarStream.close()
    jarFile.toURI.toURL
  }

  /**
   * Create a jar file that contains this set of files. All files will be located in the specified
   * directory or at the root of the jar.
   */
  def createJar(
      files: Seq[File],
      jarFile: File,
      directoryPrefix: Option[String] = None,
      mainClass: Option[String] = None): URL = {
    val manifest = mainClass match {
      case Some(mc) =>
        val m = new Manifest()
        m.getMainAttributes.putValue("Manifest-Version", "1.0")
        m.getMainAttributes.putValue("Main-Class", mc)
        m
      case None =>
        new Manifest()
    }

    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, manifest)

    for (file <- files) {
      // The `name` for the argument in `JarEntry` should use / for its separator. This is
      // ZIP specification.
      val prefix = directoryPrefix.map(d => s"$d/").getOrElse("")
      val jarEntry = new JarEntry(prefix + file.getName)
      jarStream.putNextEntry(jarEntry)

      val in = new FileInputStream(file)
      ByteStreams.copy(in, jarStream)
      in.close()
    }
    jarStream.close()
    jarFileStream.close()

    jarFile.toURI.toURL
  }

  /**
   * Run some code involving jobs submitted to the given context and assert that the jobs spilled.
   */
  def assertSpilled(sc: SparkContext, identifier: String)(body: => Unit): Unit = {
    val listener = new SpillListener
    withListener(sc, listener) { _ =>
      body
    }
    assert(listener.numSpilledStages > 0, s"expected $identifier to spill, but did not")
  }

  /**
   * Run some code involving jobs submitted to the given context and assert that the jobs
   * did not spill.
   */
  def assertNotSpilled(sc: SparkContext, identifier: String)(body: => Unit): Unit = {
    val listener = new SpillListener
    withListener(sc, listener) { _ =>
      body
    }
    assert(listener.numSpilledStages == 0, s"expected $identifier to not spill, but did")
  }

  /**
   * Asserts that exception message contains the message. Please note this checks all
   * exceptions in the tree. If a type parameter `E` is supplied, this will additionally confirm
   * that the exception is a subtype of the exception provided in the type parameter.
   */
  def assertExceptionMsg[E <: Throwable : ClassTag](
      exception: Throwable,
      msg: String,
      ignoreCase: Boolean = false): Unit = {

    val (typeMsg, typeCheck) = if (classTag[E] == classTag[Nothing]) {
      ("", (_: Throwable) => true)
    } else {
      val clazz = classTag[E].runtimeClass
      (s"of type ${clazz.getName} ", (e: Throwable) => clazz.isAssignableFrom(e.getClass))
    }

    def contain(e: Throwable, msg: String): Boolean = {
      if (ignoreCase) {
        e.getMessage.toLowerCase(Locale.ROOT).contains(msg.toLowerCase(Locale.ROOT))
      } else {
        e.getMessage.contains(msg)
      } && typeCheck(e)
    }

    var e = exception
    var contains = contain(e, msg)
    while (e.getCause != null && !contains) {
      e = e.getCause
      contains = contain(e, msg)
    }
    assert(contains,
      s"Exception tree doesn't contain the expected exception ${typeMsg}with message: $msg\n" +
        Utils.exceptionString(e))
  }

  /**
   * Test if a command is available.
   */
  def testCommandAvailable(command: String): Boolean = Utils.checkCommandAvailable(command)

  // SPARK-40053: This string needs to be updated when the
  // minimum python supported version changes.
  val minimumPythonSupportedVersion: String = "3.7.0"

  def isPythonVersionAvailable: Boolean = {
    val version = minimumPythonSupportedVersion.split('.').map(_.toInt)
    assert(version.length == 3)
    isPythonVersionAtLeast(version(0), version(1), version(2))
  }

  private def isPythonVersionAtLeast(major: Int, minor: Int, reversion: Int): Boolean = {
    val cmdSeq = if (Utils.isWindows) Seq("cmd.exe", "/C") else Seq("sh", "-c")
    val pythonSnippet = s"import sys; sys.exit(sys.version_info < ($major, $minor, $reversion))"
    Try(Process(cmdSeq :+ s"python3 -c '$pythonSnippet'").! == 0).getOrElse(false)
  }

  /**
   * Get the absolute path from the executable. This implementation was borrowed from
   * `spark/dev/sparktestsupport/shellutils.py`.
   */
  def getAbsolutePathFromExecutable(executable: String): Option[String] = {
    val command = if (Utils.isWindows) s"$executable.exe" else executable
    if (command.split(File.separator, 2).length == 1 &&
        JavaFiles.isRegularFile(Paths.get(command)) &&
        JavaFiles.isExecutable(Paths.get(command))) {
      Some(Paths.get(command).toAbsolutePath.toString)
    } else {
      sys.env("PATH").split(Pattern.quote(File.pathSeparator))
        .map(path => Paths.get(s"${StringUtils.strip(path, "\"")}${File.separator}$command"))
        .find(p => JavaFiles.isRegularFile(p) && JavaFiles.isExecutable(p))
        .map(_.toString)
    }
  }

  /**
   * Returns the response code from an HTTP(S) URL.
   */
  def httpResponseCode(
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil): Int = {
    withHttpConnection(url, method, headers = headers) { connection =>
      connection.getResponseCode()
    }
  }

  /**
   * Returns the Location header from an HTTP(S) URL.
   */
  def redirectUrl(
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil): String = {
    withHttpConnection(url, method, headers = headers) { connection =>
      connection.getHeaderField("Location");
    }
  }


  /**
   * Returns the response message from an HTTP(S) URL.
   */
  def httpResponseMessage(
    url: URL,
    method: String = "GET",
    headers: Seq[(String, String)] = Nil): String = {
    withHttpConnection(url, method, headers = headers) { connection =>
      Source.fromInputStream(connection.getInputStream, "utf-8").getLines().mkString("\n")
    }
  }

  def withHttpConnection[T](
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil)
      (fn: HttpURLConnection => T): T = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    headers.foreach { case (k, v) => connection.setRequestProperty(k, v) }

    connection match {
      // Disable cert and host name validation for HTTPS tests.
      case httpConnection: HttpsURLConnection =>
        val sslCtx = SSLContext.getInstance("SSL")
        val trustManager = new X509TrustManager {
          override def getAcceptedIssuers: Array[X509Certificate] = null

          override def checkClientTrusted(x509Certificates: Array[X509Certificate],
              s: String): Unit = {}

          override def checkServerTrusted(x509Certificates: Array[X509Certificate],
              s: String): Unit = {}
        }
        val verifier = new HostnameVerifier() {
          override def verify(hostname: String, session: SSLSession): Boolean = true
        }
        sslCtx.init(null, Array(trustManager), new SecureRandom())
        httpConnection.setSSLSocketFactory(sslCtx.getSocketFactory)
        httpConnection.setHostnameVerifier(verifier)
      case _ => // do nothing
    }

    try {
      connection.connect()
      fn(connection)
    } finally {
      connection.disconnect()
    }
  }

  /**
   * Runs some code with the given listener installed in the SparkContext. After the code runs,
   * this method will wait until all events posted to the listener bus are processed, and then
   * remove the listener from the bus.
   */
  def withListener[L <: SparkListener](sc: SparkContext, listener: L) (body: L => Unit): Unit = {
    sc.addSparkListener(listener)
    try {
      body(listener)
    } finally {
      sc.listenerBus.waitUntilEmpty()
      sc.listenerBus.removeListener(listener)
    }
  }

  def withHttpServer(resBaseDir: String = ".")(body: URL => Unit): Unit = {
    // 0 as port means choosing randomly from the available ports
    val server = new Server(new InetSocketAddress(Utils.localCanonicalHostName(), 0))
    val resHandler = new ResourceHandler()
    resHandler.setResourceBase(resBaseDir)
    val handlers = new HandlerList()
    handlers.setHandlers(Array[Handler](resHandler, new DefaultHandler()))
    server.setHandler(handlers)
    server.start()
    try {
      body(server.getURI.toURL)
    } finally {
      server.stop()
    }
  }

  /**
   * Wait until at least `numExecutors` executors are up, or throw `TimeoutException` if the waiting
   * time elapsed before `numExecutors` executors up. Exposed for testing.
   *
   * @param numExecutors the number of executors to wait at least
   * @param timeout time to wait in milliseconds
   */
  private[spark] def waitUntilExecutorsUp(
      sc: SparkContext,
      numExecutors: Int,
      timeout: Long): Unit = {
    val finishTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout)
    while (System.nanoTime() < finishTime) {
      if (sc.statusTracker.getExecutorInfos.length > numExecutors) {
        return
      }
      // Sleep rather than using wait/notify, because this is used only for testing and wait/notify
      // add overhead in the general case.
      Thread.sleep(10)
    }
    throw new TimeoutException(
      s"Can't find $numExecutors executors before $timeout milliseconds elapsed")
  }

  /**
   * config a log4j2 properties used for testsuite
   */
  def configTestLog4j2(level: String): Unit = {
    val builder = ConfigurationBuilderFactory.newConfigurationBuilder()
    val appenderBuilder = builder.newAppender("console", "CONSOLE")
      .addAttribute("target", ConsoleAppender.Target.SYSTEM_ERR)
    appenderBuilder.add(builder.newLayout("PatternLayout")
      .addAttribute("pattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex"))
    builder.add(appenderBuilder)
    builder.add(builder.newRootLogger(level).add(builder.newAppenderRef("console")))
    val configuration = builder.build()
    LogManager.getContext(false).asInstanceOf[LoggerContext].reconfigure(configuration)
  }

  /**
   * Lists files recursively.
   */
  def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val current = f.listFiles
    current ++ current.filter(_.isDirectory).flatMap(recursiveList)
  }

  /**
   * Returns the list of files at 'path' recursively. This skips files that are ignored normally
   * by MapReduce.
   */
  def listDirectory(path: File): Array[String] = {
    val result = ArrayBuffer.empty[String]
    if (path.isDirectory) {
      path.listFiles.foreach(f => result.appendAll(listDirectory(f)))
    } else {
      val c = path.getName.charAt(0)
      if (c != '.' && c != '_') result.append(path.getAbsolutePath)
    }
    result.toArray
  }

  /** Creates a temp JSON file that contains the input JSON record. */
  def createTempJsonFile(dir: File, prefix: String, jsonValue: JValue): String = {
    val file = File.createTempFile(prefix, ".json", dir)
    JavaFiles.write(file.toPath, compact(render(jsonValue)).getBytes())
    file.getPath
  }

  /** Creates a temp bash script that prints the given output. */
  def createTempScriptWithExpectedOutput(dir: File, prefix: String, output: String): String = {
    val file = File.createTempFile(prefix, ".sh", dir)
    val script = s"cat <<EOF\n$output\nEOF\n"
    Files.asCharSink(file, StandardCharsets.UTF_8).write(script)
    JavaFiles.setPosixFilePermissions(file.toPath,
      EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
    file.getPath
  }
}


/**
 * A `SparkListener` that detects whether spills have occurred in Spark jobs.
 */
private class SpillListener extends SparkListener {
  private val stageIdToTaskMetrics = new mutable.HashMap[Int, ArrayBuffer[TaskMetrics]]
  private val spilledStageIds = new mutable.HashSet[Int]

  def numSpilledStages: Int = synchronized {
    spilledStageIds.size
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    stageIdToTaskMetrics.getOrElseUpdate(
      taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics
  }

  override def onStageCompleted(stageComplete: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageComplete.stageInfo.stageId
    val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
    val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
    if (spilled) {
      spilledStageIds += stageId
    }
  }
}
