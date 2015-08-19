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
package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.storage.{ReplicationBlockSizeLimitException, StorageLevel}
import org.scalatest.{Matchers, FunSuite}

class LargePartitionCachingSuite extends FunSuite with SharedSparkContext with Matchers {

  def largePartitionRdd = sc.parallelize(1 to 1e6.toInt, 1).map{i => new Array[Byte](2.2e3.toInt)}

  //just don't want to kill the test server
  ignore("memory serialized cache large partitions") {
    largePartitionRdd.persist(StorageLevel.MEMORY_ONLY_SER).count() should be (1e6.toInt)
  }

  test("disk cache large partitions") {
    largePartitionRdd.persist(StorageLevel.DISK_ONLY).count() should be (1e6.toInt)
  }

  test("disk cache large partitions with replications") {
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 1, 512]")
      .setAppName("test-cluster")
      .set("spark.task.maxFailures", "1")
      .set("spark.akka.frameSize", "1") // set to 1MB to detect direct serialization of data
    val clusterSc = new SparkContext(conf)
    try {
      val exc = intercept[SparkException]{
        val myRDD = clusterSc.parallelize(1 to 1e6.toInt, 1).map{i => new Array[Byte](2.2e3.toInt)}
          .persist(StorageLevel.DISK_ONLY_2)
        myRDD.count()
      }
      exc.getCause() shouldBe a [ReplicationBlockSizeLimitException]
    } finally {
      clusterSc.stop()
    }
  }
}
