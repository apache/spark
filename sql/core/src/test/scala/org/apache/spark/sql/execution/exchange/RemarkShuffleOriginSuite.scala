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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RemarkShuffleOriginSuite extends SharedSparkSession with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("remark shuffle origin with join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df1 = Seq((1, 2)).toDF("c1", "c2")
      val df2 = Seq((1, 3)).toDF("c3", "c4")
      // single join
      val res1 = df1.repartition(10, $"c1")
        .join(df2, $"c1" === $"c3")
      res1.collect()
      assert(collect(res1.queryExecution.executedPlan) {
        case s: ShuffleExchangeLike if s.shuffleOrigin == ENSURE_REQUIREMENTS => s
      }.size == 2)

      // multi-join
      val df3 = Seq((1, 4)).toDF("c5", "c6")
      val res2 = df1.repartition(10, $"c1")
        .join(df2, $"c1" === $"c3")
        .join(df3, $"c1" === $"c5")
      res2.collect()
      assert(collect(res2.queryExecution.executedPlan) {
        case s: ShuffleExchangeLike if s.shuffleOrigin == ENSURE_REQUIREMENTS => s
      }.size == 3)
    }
  }

  test("remark shuffle origin with agg") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = Seq((1, 2)).toDF("c1", "c2")
      import org.apache.spark.sql.functions._
      val res1 = df.repartition(10, $"c1")
        .groupBy($"c1").agg(max($"c2"))
      res1.collect()
      assert(collect(res1.queryExecution.executedPlan) {
        case s: ShuffleExchangeLike if s.shuffleOrigin == ENSURE_REQUIREMENTS => s
      }.size == 1)
    }
  }
}
