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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.tags.SlowHiveTest
import org.apache.spark.unsafe.types.CalendarInterval

@SlowHiveTest
class SortAggregateSuite
  extends QueryTest
    with SQLTestUtils
    with TestHiveSingleton
    with ExpressionEvalHelper
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("SPARK-46536 Support GROUP BY CalendarIntervalType") {
    // forces the use of sort aggregate by using min/max functions

    val numRows = 50
    val numRepeat = 25

    val df = (0 to numRows)
      .map(i => Tuple1(new CalendarInterval(i, i, i)))
      .toDF("c0")

    for (_ <- 0 until numRepeat) {
      val shuffledDf = df.orderBy(rand())

      checkAnswer(
        shuffledDf.agg(max("c0")),
        Row(new CalendarInterval(numRows, numRows, numRows))
      )

      checkAnswer(
        shuffledDf.agg(min("c0")),
        Row(new CalendarInterval(0, 0, 0))
      )
    }
  }
}
