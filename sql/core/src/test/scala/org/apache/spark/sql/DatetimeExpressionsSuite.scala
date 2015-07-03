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

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._

class DatetimeExpressionsSuite extends QueryTest {
  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext

  import ctx.implicits._

  lazy val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")

  test("function current_date") {
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    val d1 = DateTimeUtils.fromJavaDate(df1.select(current_date()).collect().head.getDate(0))
    val d2 = DateTimeUtils.fromJavaDate(
      ctx.sql("""SELECT CURRENT_DATE()""").collect().head.getDate(0))
    val d3 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 <= d2 && d2 <= d3 && d3 - d0 <= 1)
  }

  test("function current_timestamp") {
    checkAnswer(df1.select(countDistinct(current_timestamp())), Row(1))
    // Execution in one query should return the same value
    checkAnswer(ctx.sql("""SELECT CURRENT_TIMESTAMP() = CURRENT_TIMESTAMP()"""),
      Row(true))
    assert(math.abs(ctx.sql("""SELECT CURRENT_TIMESTAMP()""").collect().head.getTimestamp(
      0).getTime - System.currentTimeMillis()) < 5000)
  }

}
