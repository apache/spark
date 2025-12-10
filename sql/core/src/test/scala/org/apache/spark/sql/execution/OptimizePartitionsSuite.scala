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

package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class OptimizePartitionsSuite extends SparkFunSuite with SharedSparkSession {

  test("TEST 1: Small Data Compaction (Coalesce)") {
    val initialDF = spark.range(10000).repartition(100)
    val optimizedDF = initialDF.optimizePartitions()
    assert(optimizedDF.rdd.getNumPartitions == 1,
      s"Expected 1 partition, got ${optimizedDF.rdd.getNumPartitions}.")
  }

  test("TEST 2: Scaling Up (Large Data Repartition)") {
    val initialDF = spark.range(500000).repartition(1)
    // initialDF size is 4MB.
    // Passing desired partition = 2MB to trigger increase in partition from 1 to 2.
    val optimizedDF = initialDF.optimizePartitions(2)
    // We expect number of partitions to increase to 2 so that each partition size is 2MB.
    assert(optimizedDF.rdd.getNumPartitions == 2,
      s"Expected scaling up, got ${optimizedDF.rdd.getNumPartitions}.")
  }
}
