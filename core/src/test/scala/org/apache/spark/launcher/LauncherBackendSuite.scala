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

package org.apache.spark.launcher


import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.Eventually._
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class LauncherBackendSuite extends SparkFunSuite with Matchers {

  private val tests = Seq(
    "local" -> "local",
    "standalone/client" -> "local-cluster[1,1,1024]")

  tests.foreach { case (name, master) =>
    test(s"$name: launcher handle") {
      testWithMaster(master)
    }

    test(s"$name: launcher handler process builder") {
      testWithMasterWithProcessBuilder(master)
    }
  }

  private def testWithMaster(master: String): Unit = {
    val env = new java.util.HashMap[String, String]()
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1")
    val handle = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setConf("spark.ui.enabled", "false")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, s"-Dtest.appender=console")
      .setMaster(master)
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(TestApp.getClass.getName().stripSuffix("$"))
      .startApplication()

    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getAppId() should not be (null)
      }

      handle.stop()

      eventually(timeout(30 seconds), interval(1000 millis)) {
        handle.getState() should be (SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }

  private def testWithMasterWithProcessBuilder(master: String): Unit = {
    val env = new java.util.HashMap[String, String]()
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1")
    // Note: this is NOT necessarily how the API is meant to be used, the user can pass in
    // an arbitrary ProcessBuilder to startApplication and it should launch it, we however
    // want to use a ProcessBuilder that will create a Spark application.
    val procBuilder = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setConf("spark.ui.enabled", "false")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, s"-Dtest.appender=console")
      .setMaster(master)
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(TestApp.getClass.getName().stripSuffix("$"))
      .createProcessBuilder()
    val handle = new SparkLauncher()
      .startApplication("TestApp", procBuilder)

    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getAppId() should not be (null)
      }

      handle.stop()

      eventually(timeout(40 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }

}

object TestApp {

  def main(args: Array[String]): Unit = {
    new SparkContext(new SparkConf()).parallelize(Seq(1)).foreach { i =>
      Thread.sleep(TimeUnit.SECONDS.toMillis(20))
    }
  }

}
