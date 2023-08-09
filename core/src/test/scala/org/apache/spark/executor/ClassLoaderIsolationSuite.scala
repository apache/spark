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

import scala.util.Properties

import org.apache.spark.{JobArtifactSet, JobArtifactState, LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.util.Utils

class ClassLoaderIsolationSuite extends SparkFunSuite with LocalSparkContext  {

  private val scalaVersion = Properties.versionNumberString
    .split("\\.")
    .take(2)
    .mkString(".")

  val jar1 = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar").toString

  // package com.example
  // object Hello { def test(): Int = 2 }
  // case class Hello(x: Int, y: Int)
  val jar2 = Thread.currentThread().getContextClassLoader
    .getResource(s"TestHelloV2_$scalaVersion.jar").toString

  // package com.example
  // object Hello { def test(): Int = 3 }
  // case class Hello(x: String)
  val jar3 = Thread.currentThread().getContextClassLoader
    .getResource(s"TestHelloV3_$scalaVersion.jar").toString

  test("Executor classloader isolation with JobArtifactSet") {
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
}
