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

package org.apache.spark.deploy.yarn

import java.io.FileInputStream
import java.util.{HashMap => JHashMap, Properties, ServiceLoader}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmitOperation
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.apache.spark.util.Utils

class YarnSparkSubmitOperationSuite extends BaseYarnClusterSuite {
  override def newYarnConfig(): YarnConfiguration = new YarnConfiguration()
  private val operation: SparkSubmitOperation =
    ServiceLoader.load(classOf[SparkSubmitOperation], Utils.getContextOrSparkClassLoader).asScala
      .head

  test("kill the application by yarn submit operation") {
    val env = new JHashMap[String, String]()
    env.put("YARN_CONF_DIR", hadoopConfDir.getAbsolutePath)

    val propsFile = createConfFile()
    val handle = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf(UI_ENABLED.key, "false")
      .setPropertiesFile(propsFile)
      .setMaster("yarn")
      .setDeployMode("client")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(mainClassName(YarnLauncherTestApp.getClass))
      .startApplication()

    try {
      eventually(timeout(3.minutes), interval(50.milliseconds)) {
        handle.getAppId should not be null
      }

      val properties = new Properties()
      val is = new FileInputStream(propsFile)
      properties.load(is)
      is.close()
      operation.kill(handle.getAppId, new SparkConf().setAll(properties.asScala))

      eventually(timeout(3.minutes), interval(50.milliseconds)) {
        handle.getState should be(SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }
}
