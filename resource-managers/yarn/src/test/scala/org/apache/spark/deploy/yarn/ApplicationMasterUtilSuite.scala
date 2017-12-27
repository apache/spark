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

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.util.ResetSystemProperties
import org.scalatest.Matchers

class ApplicationMasterUtilSuite extends SparkFunSuite with Matchers with Logging
  with ResetSystemProperties {

  test("history url with hadoop and spark substitutions") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.historyServer.address",
      "http://${hadoopconf-yarn.resourcemanager.hostname}:${spark.history.ui.port}")

    val yarnConf = new YarnConfiguration()
    yarnConf.set("yarn.resourcemanager.hostname", "rm.host.com")
    val appId = "application_123_1"
    val attemptId = appId + "_1"

    val shsAddr = ApplicationMasterUtil
      .getHistoryServerAddress(sparkConf, yarnConf, appId, attemptId)

    shsAddr shouldEqual "http://rm.host.com:18080/history/application_123_1/application_123_1_1"
  }
}
