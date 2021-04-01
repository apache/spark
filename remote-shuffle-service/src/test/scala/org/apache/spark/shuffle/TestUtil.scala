/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry

case class LeftIntKV(key: Int, value: Int)

case class RightIntKV(key: Int, value: Int)

case class LeftStringKV(key: String, value: String)

case class RightStringKV(key: String, value: String)

object TestUtil {
  def newSparkConfWithStandAloneRegistryServer(appId: String,
                                               registryServer: String): SparkConf = new SparkConf()
    .setAppName("testApp")
    .setMaster(s"local[2]")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.app.id", appId)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager")
    .set("spark.shuffle.rss.dataCenter", ServiceRegistry.DEFAULT_DATA_CENTER)
    .set("spark.shuffle.rss.cluster", appId)
    .set("spark.shuffle.rss.serviceRegistry.type", ServiceRegistry.TYPE_STANDALONE)
    .set("spark.shuffle.rss.serviceRegistry.server", registryServer)
    .set("spark.shuffle.rss.networkTimeout", "30000")
    .set("spark.shuffle.rss.networkRetries", "0")
    .set("spark.shuffle.rss.maxWaitTime", "10000")
    .set("spark.shuffle.rss.reader.dataAvailableWaitTime", "30000")
}
