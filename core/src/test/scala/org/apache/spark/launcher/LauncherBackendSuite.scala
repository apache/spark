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

import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark._
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.util.Utils

class LauncherBackendSuite extends SparkFunSuite with Matchers {

  private val tests = Seq(
    "local" -> "local",
    "standalone/client" -> "local-cluster[1,1,1024]")

  tests.foreach { case (name, master) =>
    test(s"$name: launcher handle") {
      // The tests here are failed due to the cmd length limitation up to 8K on Windows.
      assume(!Utils.isWindows)
      testWithMaster(master)
    }
  }

  private def testWithMaster(master: String): Unit = {
    val env = new java.util.HashMap[String, String]()
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1")
    val handle = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
      .setConf(UI_ENABLED.key, "false")
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, s"-Dtest.appender=console")
      .setMaster(master)
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(TestApp.getClass.getName().stripSuffix("$"))
      .startApplication()

    try {
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        handle.getAppId() should not be (null)
      }

      handle.stop()

      eventually(timeout(30.seconds), interval(100.milliseconds)) {
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
