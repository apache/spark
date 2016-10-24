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
import java.net.{URI, URL, URLClassLoader}
import java.nio.channels.{FileChannel, ReadableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, StandardOpenOption}
import java.util

import scala.concurrent.duration._
import scala.io.Source
import scala.language.implicitConversions

import com.google.common.io.Files
import org.mockito.Matchers.anyString
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Interruptor
import org.scalatest.concurrent.Timeouts._
import org.scalatest.mock.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
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
    try {
      Utils.deleteRecursively(tempDir1)
      Utils.deleteRecursively(tempDir2)
      SparkEnv.set(null)
    } finally {
      super.afterAll()
    }
  }

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, false)
    val fakeClass = classLoader.loadClass("ReplFakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("ReplFakeClassDoesNotExist").newInstance()
    }
  }

  test("resource from parent") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val resourceName: String = parentResourceNames.head
    val is = classLoader.getResourceAsStream(resourceName)
    assert(is != null, s"Resource $resourceName not found")
    val content = Source.fromInputStream(is, "UTF-8").getLines().next()
    assert(content.contains("resource"), "File doesn't contain 'resource'")
  }

  test("resources from parent") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val resourceName: String = parentResourceNames.head
    val resources: util.Enumeration[URL] = classLoader.getResources(resourceName)
    assert(resources.hasMoreElements, s"Resource $resourceName not found")
    val fileReader = Source.fromInputStream(resources.nextElement().openStream()).bufferedReader()
    assert(fileReader.readLine().contains("resource"), "File doesn't contain 'resource'")
  }

  test("fetch classes using Spark's RpcEnv") {
    val env = mock[SparkEnv]
    val rpcEnv = mock[RpcEnv]
    when(env.rpcEnv).thenReturn(rpcEnv)
    when(rpcEnv.openChannel(anyString())).thenAnswer(new Answer[ReadableByteChannel]() {
      override def answer(invocation: InvocationOnMock): ReadableByteChannel = {
        val uri = new URI(invocation.getArguments()(0).asInstanceOf[String])
        val path = Paths.get(tempDir1.getAbsolutePath(), uri.getPath().stripPrefix("/"))
        FileChannel.open(path, StandardOpenOption.READ)
      }
    })

    val classLoader = new ExecutorClassLoader(new SparkConf(), env, "spark://localhost:1234",
      getClass().getClassLoader(), false)

    val fakeClass = classLoader.loadClass("ReplFakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("ReplFakeClassDoesNotExist").newInstance()
    }
  }

}
