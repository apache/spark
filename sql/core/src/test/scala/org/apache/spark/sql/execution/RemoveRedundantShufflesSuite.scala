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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class RemoveRedundantShufflesSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def getShuffleExchangeNum(df: DataFrame): Int = {
    collect(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec => s
    }.size
  }

  test("Remove redundant shuffle") {
    withTempView("t1", "t2") {
      spark.range(10).select($"id").createOrReplaceTempView("t1")
      spark.range(20).select($"id").createOrReplaceTempView("t2")
      Seq(-1, 1000000).foreach { threshold =>
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
          val query = spark.table("t1").repartition(10).join(spark.table("t2"), "id")
          query.collect()
          val shuffleNum = getShuffleExchangeNum(query)
          if (threshold > 0) {
            assert(shuffleNum === 1)
          } else {
            assert(shuffleNum === 2)
          }
        }
      }
    }
  }

  test("Do not remove redundant shuffle if the shuffle is reused") {
    withTempView("t1", "t2", "t3") {
      spark.range(10).select($"id").createOrReplaceTempView("t1")
      spark.range(20).select($"id").createOrReplaceTempView("t2")
      spark.range(30).select($"id").createOrReplaceTempView("t3")
      Seq(-1, 1000000).foreach { threshold =>
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
          val shuffleReuse = spark.sql(
            """
              |WITH v (SELECT /*+ repartition */ * FROM t1 WHERE id > 1)
              |SELECT * FROM v JOIN t2 ON v.id = t2.id
              |UNION ALL
              |SELECT * FROM v JOIN t3 ON v.id = t3.id
              |""".stripMargin)
          shuffleReuse.collect()
          if (threshold > 0) {
            assert(getShuffleExchangeNum(shuffleReuse) === 1)
          } else {
            assert(getShuffleExchangeNum(shuffleReuse) === 4)
          }

          val repartitionReuse = spark.sql(
            """
              |WITH v (SELECT /*+ repartition */ * FROM t1 WHERE id > 1)
              |SELECT * FROM v JOIN t2 ON cast(v.id AS int) = cast(t2.id AS int)
              |UNION ALL
              |SELECT * FROM v JOIN t3 ON cast(v.id AS double) = cast(t3.id AS double)
              |""".stripMargin)
          repartitionReuse.collect()
          if (threshold > 0) {
            assert(getShuffleExchangeNum(repartitionReuse) === 1)
          } else {
            assert(getShuffleExchangeNum(repartitionReuse) === 5)
          }
        }
      }
    }
  }
}

class RemoveRedundantShufflesSuite extends RemoveRedundantShufflesSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantShufflesSuiteAE extends RemoveRedundantShufflesSuiteBase
  with EnableAdaptiveExecutionSuite
