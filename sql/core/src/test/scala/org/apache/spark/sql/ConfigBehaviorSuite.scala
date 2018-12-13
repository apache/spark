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

package org.apache.spark.sql

import org.apache.commons.math3.stat.inference.ChiSquareTest

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext


class ConfigBehaviorSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("SPARK-22160 spark.sql.execution.rangeExchange.sampleSizePerPartition") {
    // In this test, we run a sort and compute the histogram for partition size post shuffle.
    // With a high sample count, the partition size should be more evenly distributed, and has a
    // low chi-sq test value.
    // Also the whole code path for range partitioning as implemented should be deterministic
    // (it uses the partition id as the seed), so this test shouldn't be flaky.

    val numPartitions = 4

    def computeChiSquareTest(): Double = {
      val n = 10000
      // Trigger a sort
      // Range has range partitioning in its output now. To have a range shuffle, we
      // need to run a repartition first.
      val data = spark.range(0, n, 1, 1).repartition(10).sort('id.desc)
        .selectExpr("SPARK_PARTITION_ID() pid", "id").as[(Int, Long)].collect()

      // Compute histogram for the number of records per partition post sort
      val dist = data.groupBy(_._1).map(_._2.length.toLong).toArray
      assert(dist.length == 4)

      new ChiSquareTest().chiSquare(
        Array.fill(numPartitions) { n.toDouble / numPartitions },
        dist)
    }

    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString) {
      // The default chi-sq value should be low
      assert(computeChiSquareTest() < 100)

      withSQLConf(SQLConf.RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION.key -> "1") {
        // If we only sample one point, the range boundaries will be pretty bad and the
        // chi-sq value would be very high.
        assert(computeChiSquareTest() > 300)
      }
    }
  }

}
