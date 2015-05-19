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

package org.apache.spark.sql.hive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._

case class TestData2Int(a: Int, b: Int)

class HiveDataFrameAnalyticsSuiteSuite extends QueryTest {
  val testData =
    TestHive.sparkContext.parallelize(
      TestData2Int(1, 2) ::
        TestData2Int(2, 4) :: Nil).toDF()

  testData.registerTempTable("mytable")

  test("rollup") {
    checkAnswer(
      testData.rollup($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      sql("select a + b, b, sum(a - b) from mytable group by a + b, b with rollup").collect()
    )

    checkAnswer(
      testData.rollup("a", "b").agg(sum("b")),
      sql("select a, b, sum(b) from mytable group by a, b with rollup").collect()
    )
  }

  test("cube") {
    checkAnswer(
      testData.cube($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      sql("select a + b, b, sum(a - b) from mytable group by a + b, b with cube").collect()
    )

    checkAnswer(
      testData.cube("a", "b").agg(sum("b")),
      sql("select a, b, sum(b) from mytable group by a, b with cube").collect()
    )
  }

  test("spark.sql.retainGroupColumns config") {
    val oldConf = conf.getConf("spark.sql.retainGroupColumns", "true")
    try {
      conf.setConf("spark.sql.retainGroupColumns", "false")
      checkAnswer(
        testData.rollup($"a" + $"b", $"b").agg(sum($"a" - $"b")),
        sql("select sum(a-b) from mytable group by a + b, b with rollup").collect()
      )

      checkAnswer(
        testData.cube($"a" + $"b", $"b").agg(sum($"a" - $"b")),
        sql("select sum(a-b) from mytable group by a + b, b with cube").collect()
      )
    } finally {
      conf.setConf("spark.sql.retainGroupColumns", oldConf)
    }
  }
}
