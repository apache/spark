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

package org.apache.spark.repl

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.util

import com.google.common.io.Files

import scala.concurrent.duration._
import scala.io.Source
import scala.language.implicitConversions
import scala.language.postfixOps

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Interruptor
import org.scalatest.concurrent.Timeouts._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import org.apache.spark._
import org.apache.spark.util.Utils

class ExecutorClassLoaderSuite
  extends SparkFunSuite
  with BeforeAndAfterAll
  with MockitoSugar
  with Logging {

  val childClassNames = List("ReplFakeClass1", "ReplFakeClass2")
  val parentClassNames = List("ReplFakeClass1", "ReplFakeClass2", "ReplFakeClass3")
  val parentResourceNames = List("fake-resource.txt")
  var tempDir1: File = _
  var tempDir2: File = _
  var url1: String = _
  var urls2: Array[URL] = _
  var classServer: HttpServer = _

  override def beforeAll() {
    super.beforeAll()
    tempDir1 = Utils.createTempDir()
    tempDir2 = Utils.createTempDir()
    url1 = "file://" + tempDir1
    urls2 = List(tempDir2.toURI.toURL).toArray
    childClassNames.foreach(TestUtils.createCompiledClass(_, tempDir1, "1"))
    parentResourceNames.foreach { x =>
      Files.write("resource".getBytes(StandardCharsets.UTF_8), new File(tempDir2, x))
    }
    parentClassNames.foreach(TestUtils.createCompiledClass(_, tempDir2, "2"))
  }

  override def afterAll() {
    super.afterAll()
    if (classServer != null) {
      classServer.stop()
    }
    Utils.deleteRecursively(tempDir1)
    Utils.deleteRecursively(tempDir2)
    SparkEnv.set(null)
  }

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, false)
    val fakeClass = classLoader.loadClass("ReplFakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("ReplFakeClassDoesNotExist").newInstance()
    }
  }

  test("resource from parent") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    val resourceName: String = parentResourceNames.head
    val is = classLoader.getResourceAsStream(resourceName)
    assert(is != null, s"Resource $resourceName not found")
    val content = Source.fromInputStream(is, "UTF-8").getLines().next()
    assert(content.contains("resource"), "File doesn't contain 'resource'")
  }

  test("resources from parent") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    val resourceName: String = parentResourceNames.head
    val resources: util.Enumeration[URL] = classLoader.getResources(resourceName)
    assert(resources.hasMoreElements, s"Resource $resourceName not found")
    val fileReader = Source.fromInputStream(resources.nextElement().openStream()).bufferedReader()
    assert(fileReader.readLine().contains("resource"), "File doesn't contain 'resource'")
  }

  test("failing to fetch classes from HTTP server should not leak resources (SPARK-6209)") {
    // This is a regression test for SPARK-6209, a bug where each failed attempt to load a class
    // from the driver's class server would leak a HTTP connection, causing the class server's
    // thread / connection pool to be exhausted.
    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf)
    classServer = new HttpServer(conf, tempDir1, securityManager)
    classServer.start()
    // ExecutorClassLoader uses SparkEnv's SecurityManager, so we need to mock this
    val mockEnv = mock[SparkEnv]
    when(mockEnv.securityManager).thenReturn(securityManager)
    SparkEnv.set(mockEnv)
    // Create an ExecutorClassLoader that's configured to load classes from the HTTP server
    val parentLoader = new URLClassLoader(Array.empty, null)
    val classLoader = new ExecutorClassLoader(conf, classServer.uri, parentLoader, false)
    classLoader.httpUrlConnectionTimeoutMillis = 500
    // Check that this class loader can actually load classes that exist
    val fakeClass = classLoader.loadClass("ReplFakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    // Try to perform a full GC now, since GC during the test might mask resource leaks
    System.gc()
    // When the original bug occurs, the test thread becomes blocked in a classloading call
    // and does not respond to interrupts.  Therefore, use a custom ScalaTest interruptor to
    // shut down the HTTP server when the test times out
    val interruptor: Interruptor = new Interruptor {
      override def apply(thread: Thread): Unit = {
        classServer.stop()
        classServer = null
        thread.interrupt()
      }
    }
    def tryAndFailToLoadABunchOfClasses(): Unit = {
      // The number of trials here should be much larger than Jetty's thread / connection limit
      // in order to expose thread or connection leaks
      for (i <- 1 to 1000) {
        if (Thread.currentThread().isInterrupted) {
          throw new InterruptedException()
        }
        // Incorporate the iteration number into the class name in order to avoid any response
        // caching that might be added in the future
        intercept[ClassNotFoundException] {
          classLoader.loadClass(s"ReplFakeClassDoesNotExist$i").newInstance()
        }
      }
    }
    failAfter(10 seconds)(tryAndFailToLoadABunchOfClasses())(interruptor)
  }

}
