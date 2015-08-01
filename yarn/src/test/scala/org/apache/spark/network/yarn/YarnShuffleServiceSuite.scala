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
package org.apache.spark.network.yarn

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.shuffle.TestUtil
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo

class YarnShuffleServiceSuite extends SparkFunSuite with Matchers {
  private[yarn] var yarnConfig: YarnConfiguration = new YarnConfiguration

  {
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle");
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      "org.apache.spark.network.yarn.YarnShuffleService");

    yarnConfig.get("yarn.nodemanager.local-dirs").split(",").foreach { dir =>
      println("making dir " + dir)
      val d = new File(dir)
      if (d.exists()) {
        FileUtils.deleteDirectory(d)
      }
      FileUtils.forceMkdir(d)
    }
  }

  test("executor state kept across NM restart") {
    val service: YarnShuffleService = new YarnShuffleService
    service.init(yarnConfig)
    val appId = ApplicationId.newInstance(0, 0)
    val appData: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", appId, null)
    service.initializeApplication(appData)

    val execStateFile = service.registeredExecutorFile
    execStateFile should not be (null)
    execStateFile.exists() should be (false)

    val blockHandler = service.blockHandler
    val blockResolver = TestUtil.getBlockResolver(blockHandler)
    TestUtil.registeredExecutorFile(blockResolver) should be (execStateFile)

    val shuffleInfo = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, "sort")
    blockResolver.registerExecutor(appId.toString, "exec-1", shuffleInfo)
    val executor = TestUtil.getExecutorInfo(appId.toString, "exec-1", blockResolver)
    executor should be (Some(shuffleInfo))

    execStateFile.exists() should be (true)

    // now we pretend the shuffle service goes down, and comes back up
    service.stop()

    val s2: YarnShuffleService = new YarnShuffleService
    s2.init(yarnConfig)
    service.registeredExecutorFile should be (execStateFile)

    val handler2 = service.blockHandler
    val resolver2 = TestUtil.getBlockResolver(handler2)

    // until we initial the application, don't know about any executors

//    TestUtil.getExecutorInfo(appId.toString, "exec-1", blockResolver) should be (None)

    s2.initializeApplication(appData)
    val ex2 = TestUtil.getExecutorInfo(appId.toString, "exec-1", resolver2)
    ex2 should be (Some(shuffleInfo))
  }
}
