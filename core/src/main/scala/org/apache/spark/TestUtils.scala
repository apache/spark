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
import java.net.{HttpURLConnection, URI, URL}
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.{Arrays, Properties}
import java.util.concurrent.{TimeoutException, TimeUnit}
import java.util.jar.{JarEntry, JarOutputStream}
import javax.net.ssl._
import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import com.google.common.io.{ByteStreams, Files}
import org.apache.log4j.PropertyConfigurator

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * Utilities for tests. Included in main codebase since it's used by multiple
 * projects.
 *
 * TODO: See if we can move this to the test codebase by specifying
 * test dependencies between projects.
 */
private[spark] object TestUtils {

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
  def createJar(files: Seq[File], jarFile: File, directoryPrefix: Option[String] = None): URL = {
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest())

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

  // Adapted from the JavaCompiler.java doc examples
  private val SOURCE = JavaFileObject.Kind.SOURCE
  private def createURI(name: String) = {
    URI.create(s"string:///${name.replace(".", "/")}${SOURCE.extension}")
  }

  private[spark] class JavaSourceFromString(val name: String, val code: String)
    extends SimpleJavaFileObject(createURI(name), SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): String = code
  }

  /** Creates a compiled class with the source file. Class file will be placed in destDir. */
  def createCompiledClass(
      className: String,
      destDir: File,
      sourceFile: JavaSourceFromString,
      classpathUrls: Seq[URL]): File = {
    val compiler = ToolProvider.getSystemJavaCompiler

    // Calling this outputs a class file in pwd. It's easier to just rename the files than
    // build a custom FileManager that controls the output location.
    val options = if (classpathUrls.nonEmpty) {
      Seq("-classpath", classpathUrls.map { _.getFile }.mkString(File.pathSeparator))
    } else {
      Seq.empty
    }
    compiler.getTask(null, null, null, options.asJava, null, Arrays.asList(sourceFile)).call()

    val fileName = className + ".class"
    val result = new File(fileName)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath())
    val out = new File(destDir, fileName)

    // renameTo cannot handle in and out files in different filesystems
    // use google's Files.move instead
    Files.move(result, out)

    assert(out.exists(), "Destination file not moved: " + out.getAbsolutePath())
    out
  }

  /** Creates a compiled class with the given name. Class file will be placed in destDir. */
  def createCompiledClass(
      className: String,
      destDir: File,
      toStringValue: String = "",
      baseClass: String = null,
      classpathUrls: Seq[URL] = Seq.empty): File = {
    val extendsText = Option(baseClass).map { c => s" extends ${c}" }.getOrElse("")
    val sourceFile = new JavaSourceFromString(className,
      "public class " + className + extendsText + " implements java.io.Serializable {" +
      "  @Override public String toString() { return \"" + toStringValue + "\"; }}")
    createCompiledClass(className, destDir, sourceFile, classpathUrls)
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
   * Test if a command is available.
   */
  def testCommandAvailable(command: String): Boolean = {
    val attempt = Try(Process(command).run(ProcessLogger(_ => ())).exitValue())
    attempt.isSuccess && attempt.get == 0
  }

  /**
   * Returns the response code from an HTTP(S) URL.
   */
  def httpResponseCode(
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil): Int = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    headers.foreach { case (k, v) => connection.setRequestProperty(k, v) }

    // Disable cert and host name validation for HTTPS tests.
    if (connection.isInstanceOf[HttpsURLConnection]) {
      val sslCtx = SSLContext.getInstance("SSL")
      val trustManager = new X509TrustManager {
        override def getAcceptedIssuers(): Array[X509Certificate] = null
        override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) {}
        override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) {}
      }
      val verifier = new HostnameVerifier() {
        override def verify(hostname: String, session: SSLSession): Boolean = true
      }
      sslCtx.init(null, Array(trustManager), new SecureRandom())
      connection.asInstanceOf[HttpsURLConnection].setSSLSocketFactory(sslCtx.getSocketFactory())
      connection.asInstanceOf[HttpsURLConnection].setHostnameVerifier(verifier)
    }

    try {
      connection.connect()
      connection.getResponseCode()
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
      sc.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(10))
      sc.listenerBus.removeListener(listener)
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
   * config a log4j properties used for testsuite
   */
  def configTestLog4j(level: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    PropertyConfigurator.configure(pro)
  }

  /**
   * Lists files recursively.
   */
  def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val current = f.listFiles
    current ++ current.filter(_.isDirectory).flatMap(recursiveList)
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
