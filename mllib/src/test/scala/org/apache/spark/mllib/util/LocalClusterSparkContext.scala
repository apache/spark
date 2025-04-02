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

package org.apache.spark.mllib.util

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config.EXECUTOR_MEMORY
import org.apache.spark.internal.config.Network.RPC_MESSAGE_MAX_SIZE

trait LocalClusterSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 1, 512]")
      .setAppName("test-cluster")
      .set(EXECUTOR_MEMORY.key, "512m")
      .set(RPC_MESSAGE_MAX_SIZE, 1) // set to 1MB to detect direct serialization of data
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    try {
      if (sc != null) {
        sc.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}
