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

import org.apache.spark.SharedSparkContext
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class LargePartitionCachingSuite extends FunSuite with SharedSparkContext {

  def largePartitionRdd = sc.parallelize(1 to 1e6.toInt, 1).map{i => new Array[Byte](2.2e3.toInt)}

  //just don't want to kill the test server
  ignore("memory serialized cache large partitions") {
    largePartitionRdd.persist(StorageLevel.MEMORY_ONLY_SER).count()
  }

  test("disk cache large partitions") {
    largePartitionRdd.persist(StorageLevel.DISK_ONLY).count()
  }

  test("disk cache large partitions with replications") {
    pending
  }
}
