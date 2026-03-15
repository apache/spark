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

  test("TEST 1: Downscaling (Coalesce behavior)") {
    val rowCount = 10000
    // Force 100 partitions on tiny data (~80KB)
    val initialDF = spark.range(rowCount).repartition(100)
    assert(initialDF.rdd.getNumPartitions == 100)

    // Optimize with default 128MB. Should drop to 1 partition.
    val optimizedDF = initialDF.optimizePartitions()

    // Check Partition Count
    assert(optimizedDF.rdd.getNumPartitions == 1,
      s"Expected 1 partition, got ${optimizedDF.rdd.getNumPartitions}.")

    // Check Data Integrity (Crucial!)
    assert(optimizedDF.count() == rowCount, "Data loss detected after optimization!")
  }

  test("TEST 2: Upscaling (Repartition behavior)") {
    val rowCount = 500000
    // 500,000 Longs (8 bytes) = ~4,000,000 bytes (approx 3.8 MB)
    // We force it into 1 partition initially
    val initialDF = spark.range(rowCount).repartition(1)

    // We set target to 2MB.
    // Logic: 4MB Total / 2MB Target = 2 Partitions needed.
    val optimizedDF = initialDF.optimizePartitions(2)

    // Check Partition Count
    assert(optimizedDF.rdd.getNumPartitions == 2,
      s"Expected scaling up to 2 partitions, got ${optimizedDF.rdd.getNumPartitions}.")

    // Check Data Integrity
    assert(optimizedDF.count() == rowCount, "Data count mismatch after scaling up!")
  }

  test("TEST 3: Idempotency (Already Optimal)") {
    // Data is ~4MB. Current partitions = 2. Target = 2MB.
    // 4MB / 2MB = 2.
    // Since Calculated (2) == Current (2), the rule should do nothing (return child).
    val rowCount = 500000
    val initialDF = spark.range(rowCount).repartition(2)

    val optimizedDF = initialDF.optimizePartitions(2)

    assert(optimizedDF.rdd.getNumPartitions == 2)
    assert(optimizedDF.count() == rowCount)

    // Optional: Verify the plan didn't change (Optimization overhead check)
    // We compare logical plans to see if a new Repartition node was added
    assert(initialDF.queryExecution.optimizedPlan == optimizedDF.queryExecution.optimizedPlan,
      "Plan should not change if partition count is already optimal")
  }

  test("TEST 4: Edge Case - Zero or Negative Target") {
    val initialDF = spark.range(100)

    // 1. Check for Zero
    // We expect the code to throw IllegalArgumentException
    val e1 = intercept[IllegalArgumentException] {
      // We call .rdd or .collect() to force the optimizer to run (if validation is in the Rule)
      initialDF.optimizePartitions(0).rdd
    }
    assert(e1.getMessage.contains("targetMB must be positive"))

    // 2. Check for Negative
    val e2 = intercept[IllegalArgumentException] {
      initialDF.optimizePartitions(-5).rdd
    }
    assert(e2.getMessage.contains("targetMB must be positive"))
  }

  test("TEST 5: Edge Case - Empty DataFrame") {
    val emptyDF = spark.range(0).repartition(10)

    // Optimization should recognize size is 0.
    // It might return 1 (your rule) or 0 (PropagateEmptyRelation optimization).
    // Both are valid "optimized" states compared to 10.
    val optimizedDF = emptyDF.optimizePartitions()

    val finalPartitions = optimizedDF.rdd.getNumPartitions
    assert(finalPartitions <= 1, s"Expected <= 1 partition for empty data, got $finalPartitions")

    assert(optimizedDF.count() == 0)
  }
}
