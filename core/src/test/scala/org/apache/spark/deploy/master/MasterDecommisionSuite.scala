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

package org.apache.spark.deploy.master

import java.net.{HttpURLConnection, URI}

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.deploy._
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.UI._
import org.apache.spark.util.Utils

class MasterDecommisionSuite extends MasterSuiteBase {
  test("SPARK-46888: master should reject worker kill request if decommision is disabled") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
      .set(DECOMMISSION_ENABLED, false)
      .set(MASTER_UI_DECOMMISSION_ALLOW_MODE, "ALLOW")
    val localCluster = LocalSparkCluster(1, 1, 512, conf)
    localCluster.start()
    val masterUrl = s"http://${Utils.localHostNameForURI()}:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        val url = new URI(s"$masterUrl/workers/kill/?host=${Utils.localHostNameForURI()}").toURL
        val conn = url.openConnection().asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("POST")
        assert(conn.getResponseCode === 405)
      }
    } finally {
      localCluster.stop()
    }
  }

  test("All workers on a host should be decommissioned") {
    testWorkerDecommissioning(2, 2, Seq("LoCalHost", "localHOST"))
  }

  test("No workers should be decommissioned with invalid host") {
    testWorkerDecommissioning(2, 0, Seq("NoSuchHost1", "NoSuchHost2"))
  }

  test("Only worker on host should be decommissioned") {
    testWorkerDecommissioning(1, 1, Seq("lOcalHost", "NoSuchHost"))
  }
}
