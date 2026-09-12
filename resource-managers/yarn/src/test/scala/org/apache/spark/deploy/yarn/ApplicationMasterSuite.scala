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
import org.apache.spark.deploy.yarn.config._

class ApplicationMasterSuite extends SparkFunSuite {

  test("history url with hadoop and spark substitutions") {
    val host = "rm.host.com"
    val port = 18080
    val sparkConf = new SparkConf()

    sparkConf.set(HISTORY_SERVER_ADDRESS,
      "http://${hadoopconf-yarn.resourcemanager.hostname}:${spark.history.ui.port}")
    val yarnConf = new YarnConfiguration()
    yarnConf.set("yarn.resourcemanager.hostname", host)
    val appId = "application_123_1"
    val attemptId = appId + "_1"

    val shsAddr = ApplicationMaster
      .getHistoryServerAddress(sparkConf, yarnConf, appId, attemptId)

    assert(shsAddr === s"http://${host}:${port}/history/${appId}/${attemptId}")
  }

  test("SPARK-59312: amIpFilterParams adds TRUST_PROXY_USER_COOKIE only when not trusted") {
    val baseParams = Map("PROXY_HOSTS" -> "host", "PROXY_URI_BASES" -> "uri")

    // Trusted (the default): parameters are unchanged from the original behavior.
    assert(ApplicationMaster.amIpFilterParams(baseParams, trustProxyUserCookie = true)
      === baseParams)

    // Not trusted: the TRUST_PROXY_USER_COOKIE=false init parameter is added.
    val untrusted = ApplicationMaster.amIpFilterParams(baseParams, trustProxyUserCookie = false)
    assert(untrusted === baseParams + (AmIpFilter.TRUST_PROXY_USER_PARAM -> "false"))
    assert(untrusted(AmIpFilter.TRUST_PROXY_USER_PARAM) === "false")
  }
}
