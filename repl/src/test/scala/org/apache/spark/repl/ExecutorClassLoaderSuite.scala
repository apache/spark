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

import java.io.{File, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{URI, URL, URLClassLoader}
import java.nio.channels.{FileChannel, ReadableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, StandardOpenOption}
import java.util
import java.util.Collections
import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}

import scala.io.Source

import com.google.common.io.Files
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.TestUtils.JavaSourceFromString
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir1 = Utils.createTempDir()
    tempDir2 = Utils.createTempDir()
    url1 = tempDir1.toURI.toURL.toString
    urls2 = List(tempDir2.toURI.toURL).toArray
    childClassNames.foreach(TestUtils.createCompiledClass(_, tempDir1, "1"))
    parentResourceNames.foreach { x =>
      Files.write("resource".getBytes(StandardCharsets.UTF_8), new File(tempDir2, x))
    }
    parentClassNames.foreach(TestUtils.createCompiledClass(_, tempDir2, "2"))
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir1)
      Utils.deleteRecursively(tempDir2)
      SparkEnv.set(null)
    } finally {
      super.afterAll()
    }
  }

  test("child over system classloader") {
    // JavaFileObject for scala.Option class
    val scalaOptionFile = new SimpleJavaFileObject(
      URI.create(s"string:///scala/Option.java"),
      JavaFileObject.Kind.SOURCE) {

      override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = {
        "package scala; class Option {}"
      }
    }
    // compile fake scala.Option class
    ToolProvider
      .getSystemJavaCompiler
      .getTask(null, null, null, null, null, Collections.singletonList(scalaOptionFile)).call()

    // create 'scala' dir in tempDir1
    val scalaDir = new File(tempDir1, "scala")
    assert(scalaDir.mkdir(), s"Failed to create 'scala' directory in $tempDir1")

    // move the generated class into scala dir
    val filename = "Option.class"
    val result = new File(filename)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath)

    val out = new File(scalaDir, filename)
    Files.move(result, out)
    assert(out.exists(), "Destination file not moved: " + out.getAbsolutePath)

    // construct class loader tree
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(
      new SparkConf(), null, url1, parentLoader, true)

    // scalastyle:off classforname
    // load 'scala.Option', using Class.forName to do the exact same behavior as
    // what JavaDeserializationStream does
    val optionClass = Class.forName("scala.Option", false, classLoader)
    // scalastyle:on classforname

    assert(optionClass.getClassLoader == classLoader,
      "scala.Option didn't come from ExecutorClassLoader")
  }

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass2").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, false)
    val fakeClass = classLoader.loadClass("ReplFakeClass1").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass3").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("ReplFakeClassDoesNotExist").getConstructor().newInstance()
    }
  }

  test("resource from parent") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val resourceName: String = parentResourceNames.head
    val is = classLoader.getResourceAsStream(resourceName)
    assert(is != null, s"Resource $resourceName not found")

    val bufferedSource = Source.fromInputStream(is, StandardCharsets.UTF_8.name())
    Utils.tryWithSafeFinally {
      val content = bufferedSource.getLines().next()
      assert(content.contains("resource"), "File doesn't contain 'resource'")
    } {
      bufferedSource.close()
    }
  }

  test("resources from parent") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), null, url1, parentLoader, true)
    val resourceName: String = parentResourceNames.head
    val resources: util.Enumeration[URL] = classLoader.getResources(resourceName)
    assert(resources.hasMoreElements, s"Resource $resourceName not found")

    val bufferedSource = Source.fromInputStream(resources.nextElement().openStream())
    Utils.tryWithSafeFinally {
      val fileReader = bufferedSource.bufferedReader()
      assert(fileReader.readLine().contains("resource"), "File doesn't contain 'resource'")
    } {
      bufferedSource.close()
    }
  }

  test("fetch classes using Spark's RpcEnv") {
    val env = mock[SparkEnv]
    val rpcEnv = mock[RpcEnv]
    when(env.rpcEnv).thenReturn(rpcEnv)
    when(rpcEnv.openChannel(anyString())).thenAnswer((invocation: InvocationOnMock) => {
      val uri = new URI(invocation.getArguments()(0).asInstanceOf[String])
      val path = Paths.get(tempDir1.getAbsolutePath(), uri.getPath().stripPrefix("/"))
      if (path.toFile.exists()) {
        FileChannel.open(path, StandardOpenOption.READ)
      } else {
        val channel = mock[ReadableByteChannel]
        when(channel.read(any()))
          .thenThrow(new RuntimeException(s"Stream '${uri.getPath}' was not found."))
        channel
      }
    })

    val classLoader = new ExecutorClassLoader(new SparkConf(), env, "spark://localhost:1234",
      getClass().getClassLoader(), false)

    val fakeClass = classLoader.loadClass("ReplFakeClass2").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("ReplFakeClassDoesNotExist").getConstructor().newInstance()
    }

    // classLoader.getResourceAsStream() should also be able to fetch the Class file
    val fakeClassInputStream = classLoader.getResourceAsStream("ReplFakeClass2.class")
    try {
      val magic = new Array[Byte](4)
      fakeClassInputStream.read(magic)
      // first 4 bytes should match the magic number of Class file
      assert(magic === Array[Byte](0xCA.toByte, 0xFE.toByte, 0xBA.toByte, 0xBE.toByte))
    } finally {
      if (fakeClassInputStream != null) fakeClassInputStream.close()
    }
  }

  test("nonexistent class and transient errors should cause different errors") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("executor-class-loader-test")
      .set("spark.network.timeout", "11s")
      .set("spark.network.timeoutInterval", "11s")
      .set("spark.repl.class.outputDir", tempDir1.getAbsolutePath)
    val sc = new SparkContext(conf)
    try {
      val replClassUri = sc.conf.get("spark.repl.class.uri")

      // Create an RpcEnv for executor
      val rpcEnv = RpcEnv.create(
        SparkEnv.executorSystemName,
        "localhost",
        "localhost",
        0,
        sc.conf,
        new SecurityManager(conf), 0, clientMode = true)

      try {
        val env = mock[SparkEnv]
        when(env.rpcEnv).thenReturn(rpcEnv)

        val classLoader = new ExecutorClassLoader(
          conf,
          env,
          replClassUri,
          getClass().getClassLoader(),
          false)

        // Test loading a nonexistent class
        intercept[java.lang.ClassNotFoundException] {
          classLoader.loadClass("NonexistentClass")
        }

        // Stop SparkContext to simulate transient errors in executors
        sc.stop()

        val e = intercept[RemoteClassLoaderError] {
          classLoader.loadClass("ThisIsAClassName")
        }
        assert(e.getMessage.contains("ThisIsAClassName"))
        // RemoteClassLoaderError must not be LinkageError nor ClassNotFoundException. Otherwise,
        // JVM will cache it and doesn't retry to load a class.
        assert(!(classOf[LinkageError].isAssignableFrom(e.getClass)))
        assert(!(classOf[ClassNotFoundException].isAssignableFrom(e.getClass)))
      } finally {
        rpcEnv.shutdown()
        rpcEnv.awaitTermination()
      }
    } finally {
      sc.stop()
    }
  }

  test("SPARK-20547 ExecutorClassLoader should not throw ClassNotFoundException without " +
    "acknowledgment from driver") {
    val tempDir = Utils.createTempDir()
    try {
      // Create two classes, "TestClassB" calls "TestClassA", so when calling "TestClassB.foo", JVM
      // will try to load "TestClassA".
      val sourceCodeOfClassA =
        """public class TestClassA implements java.io.Serializable {
          |  @Override public String toString() { return "TestClassA"; }
          |}""".stripMargin
      val sourceFileA = new JavaSourceFromString("TestClassA", sourceCodeOfClassA)
      TestUtils.createCompiledClass(
        sourceFileA.name, tempDir, sourceFileA, Seq(tempDir.toURI.toURL))

      val sourceCodeOfClassB =
        """public class TestClassB implements java.io.Serializable {
        |  public String foo() { return new TestClassA().toString(); }
        |  @Override public String toString() { return "TestClassB"; }
        |}""".stripMargin
      val sourceFileB = new JavaSourceFromString("TestClassB", sourceCodeOfClassB)
      TestUtils.createCompiledClass(
        sourceFileB.name, tempDir, sourceFileB, Seq(tempDir.toURI.toURL))

      val env = mock[SparkEnv]
      val rpcEnv = mock[RpcEnv]
      when(env.rpcEnv).thenReturn(rpcEnv)
      when(rpcEnv.openChannel(anyString())).thenAnswer(new Answer[ReadableByteChannel]() {
        private var count = 0

        override def answer(invocation: InvocationOnMock): ReadableByteChannel = {
          val uri = new URI(invocation.getArguments()(0).asInstanceOf[String])
          val classFileName = uri.getPath().stripPrefix("/")
          if (count == 0 && classFileName == "TestClassA.class") {
            count += 1
            // Let the first attempt to load TestClassA fail with an IOException
            val channel = mock[ReadableByteChannel]
            when(channel.read(any())).thenThrow(new IOException("broken pipe"))
            channel
          }
          else {
            val path = Paths.get(tempDir.getAbsolutePath(), classFileName)
            FileChannel.open(path, StandardOpenOption.READ)
          }
        }
      })

      val classLoader = new ExecutorClassLoader(new SparkConf(), env, "spark://localhost:1234",
        getClass().getClassLoader(), false)

      def callClassBFoo(): String = {
        // scalastyle:off classforname
        val classB = Class.forName("TestClassB", true, classLoader)
        // scalastyle:on classforname
        val instanceOfTestClassB = classB.newInstance()
        assert(instanceOfTestClassB.toString === "TestClassB")
        classB.getMethod("foo").invoke(instanceOfTestClassB).asInstanceOf[String]
      }

      // Reflection will wrap the exception with InvocationTargetException
      val e = intercept[InvocationTargetException] {
        callClassBFoo()
      }
      // "TestClassA" cannot be loaded because of IOException
      assert(e.getCause.isInstanceOf[RemoteClassLoaderError])
      assert(e.getCause.getCause.isInstanceOf[IOException])
      assert(e.getCause.getMessage.contains("TestClassA"))

      // We should be able to re-load TestClassA for IOException
      assert(callClassBFoo() === "TestClassA")
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
