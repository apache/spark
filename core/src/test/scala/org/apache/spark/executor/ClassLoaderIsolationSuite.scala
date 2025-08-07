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

package org.apache.spark.executor

import java.io.File
import java.net.URL

import scala.util.Properties

import org.apache.spark.{JobArtifactSet, JobArtifactState, LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.util.{MutableURLClassLoader, Utils}


class ClassLoaderIsolationSuite extends SparkFunSuite with LocalSparkContext  {

  private val scalaVersion = Properties.versionNumberString
    .split("\\.")
    .take(2)
    .mkString(".")

  private val jarURL1 = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar")
  private lazy val jar1 = jarURL1.toString

  // package com.example
  // object Hello { def test(): Int = 2 }
  // case class Hello(x: Int, y: Int)
  private val jarURL2 = Thread.currentThread().getContextClassLoader
    .getResource(s"TestHelloV2_$scalaVersion.jar")
  private lazy val jar2 = jarURL2.toString

  // package com.example
  // object Hello { def test(): Int = 3 }
  // case class Hello(x: String)
  private val jarURL3 = Thread.currentThread().getContextClassLoader
    .getResource(s"TestHelloV3_$scalaVersion.jar")
  private lazy val jar3 = jarURL3.toString

  test("Executor classloader isolation with JobArtifactSet") {
    assume(jarURL1 != null)
    assume(jarURL2 != null)
    assume(jarURL3 != null)

    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    sc.addJar(jar1)
    sc.addJar(jar2)
    sc.addJar(jar3)

    // TestHelloV2's test method returns '2'
    val artifactSetWithHelloV2 = new JobArtifactSet(
      Some(JobArtifactState(uuid = "hello2", replClassDirUri = None)),
      jars = Map(jar2 -> 1L),
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithHelloV2.state.get) {
      sc.addJar(jar2)
      sc.parallelize(1 to 1).foreach { i =>
        val cls = Utils.classForName("com.example.Hello$")
        val module = cls.getField("MODULE$").get(null)
        val result = cls.getMethod("test").invoke(module).asInstanceOf[Int]
        if (result != 2) {
          throw new RuntimeException("Unexpected result: " + result)
        }
      }
    }

    // TestHelloV3's test method returns '3'
    val artifactSetWithHelloV3 = new JobArtifactSet(
      Some(JobArtifactState(uuid = "hello3", replClassDirUri = None)),
      jars = Map(jar3 -> 1L),
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithHelloV3.state.get) {
      sc.addJar(jar3)
      sc.parallelize(1 to 1).foreach { i =>
        val cls = Utils.classForName("com.example.Hello$")
        val module = cls.getField("MODULE$").get(null)
        val result = cls.getMethod("test").invoke(module).asInstanceOf[Int]
        if (result != 3) {
          throw new RuntimeException("Unexpected result: " + result)
        }
      }
    }

    // Should not be able to see any "Hello" class if they're excluded from the artifact set
    val artifactSetWithoutHello = new JobArtifactSet(
      Some(JobArtifactState(uuid = "Jar 1", replClassDirUri = None)),
      jars = Map(jar1 -> 1L),
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithoutHello.state.get) {
      sc.addJar(jar1)
      sc.parallelize(1 to 1).foreach { i =>
        try {
          Utils.classForName("com.example.Hello$")
          throw new RuntimeException("Import should fail")
        } catch {
          case _: ClassNotFoundException =>
        }
      }
    }
  }

  test("SPARK-51537 Executor isolation session classloader inherits from " +
    "default session classloader") {
    assume(jarURL2 != null)
    sc = new SparkContext(new SparkConf()
      .setAppName("test")
      .setMaster("local")
      .set("spark.jars", jar2))

    // TestHelloV2's test method returns '2'
    val artifactSetWithHelloV2 = new JobArtifactSet(
      Some(JobArtifactState(uuid = "hello2", replClassDirUri = None)),
      jars = Map.empty,
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(artifactSetWithHelloV2.state.get) {
      sc.parallelize(1 to 1).foreach { _ =>
        val cls = Utils.classForName("com.example.Hello$")
        val module = cls.getField("MODULE$").get(null)
        val result = cls.getMethod("test").invoke(module).asInstanceOf[Int]
        if (result != 2) {
          throw new RuntimeException("Unexpected result: " + result)
        }
      }
    }
  }

  test("SPARK-51537 Executor isolation avoids reloading plugin jars") {
    val tempDir = Utils.createTempDir()

    val testCodeBody =
      s"""
         | public static boolean flag = false;
         |""".stripMargin

    val compiledTestCode = TestUtils.createCompiledClass(
      "TestFoo",
      tempDir,
      "",
      null,
      Seq.empty,
      Seq.empty,
      testCodeBody)

    // Initialize the static variable flag in TestFoo when loading plugin at the first time.
    // If the plugin is reloaded, the TestFoo.flag will be set to false by default.
    val executorPluginCodeBody =
      s"""
         |@Override
         |public void init(
         |    org.apache.spark.api.plugin.PluginContext ctx,
         |    java.util.Map<String, String> extraConf) {
         |  TestFoo.flag = true;
         |}
      """.stripMargin

    val thisClassPath =
      sys.props("java.class.path").split(File.pathSeparator).map(p => new File(p).toURI.toURL)

    val compiledExecutorPlugin = TestUtils.createCompiledClass(
      "TestExecutorPlugin",
      tempDir,
      "",
      null,
      Seq(tempDir.toURI.toURL) ++ thisClassPath,
      Seq("org.apache.spark.api.plugin.ExecutorPlugin"),
      executorPluginCodeBody)

    val sparkPluginCodeBody =
      """
        |@Override
        |public org.apache.spark.api.plugin.ExecutorPlugin executorPlugin() {
        |  return new TestExecutorPlugin();
        |}
        |
        |@Override
        |public org.apache.spark.api.plugin.DriverPlugin driverPlugin() { return null; }
      """.stripMargin

    val compiledSparkPlugin = TestUtils.createCompiledClass(
      "TestSparkPlugin",
      tempDir,
      "",
      null,
      Seq(tempDir.toURI.toURL) ++ thisClassPath,
      Seq("org.apache.spark.api.plugin.SparkPlugin"),
      sparkPluginCodeBody)

    val jarUrl = TestUtils.createJar(
      Seq(compiledSparkPlugin, compiledExecutorPlugin, compiledTestCode),
      new File(tempDir, "testplugin.jar"))

    def getClassLoader: MutableURLClassLoader = {
      val loader = new MutableURLClassLoader(new Array[URL](0),
        Thread.currentThread.getContextClassLoader)
      Thread.currentThread.setContextClassLoader(loader)
      loader
    }
    // SparkContext does not add plugin jars specified by `spark.jars` configuration
    // to the classpath, causing ClassNotFoundException when initializing plugins
    // in SparkContext. We manually add the jars to the ClassLoader to resolve this.
    val loader = getClassLoader
    loader.addURL(jarUrl)

    sc = new SparkContext(new SparkConf()
      .setAppName("avoid-reloading-plugins")
      .setMaster("local-cluster[1, 1, 1024]")
      .set("spark.jars", jarUrl.toString)
      .set("spark.plugins", "TestSparkPlugin"))

    val jobArtifactSet = new JobArtifactSet(
      Some(JobArtifactState(uuid = "avoid-reloading-plugins", replClassDirUri = None)),
      jars = Map.empty,
      files = Map.empty,
      archives = Map.empty
    )

    JobArtifactSet.withActiveJobArtifactState(jobArtifactSet.state.get) {
      sc.parallelize(1 to 1).foreach { _ =>
        val cls1 = Utils.classForName("TestFoo")
        val z = cls1.getField("flag").getBoolean(null)
        // If the plugin has been reloaded, the TestFoo.flag will be false.
        if (!z) {
          throw new RuntimeException("The spark plugin is reloaded")
        }
      }
    }
  }
}
