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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.SharedHiveContext

// TODO ideally we should put the test suite into the package `sql`, as
// `hive` package is optional in compiling, however, `SQLContext.sql` doesn't
// support the `cube` or `rollup` yet.
class HiveDataFrameAnalyticsSuite extends QueryTest with SharedHiveContext {
  import testImplicits._

  private var _testData: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _testData = Seq((1, 2), (2, 4)).toDF("a", "b")
    ctx.registerDataFrameAsTable(_testData, "mytable")
  }

  override def afterAll(): Unit = {
    try {
      ctx.dropTempTable("mytable")
    } finally {
      super.afterAll()
    }
  }

  test("rollup") {
    checkAnswer(
      _testData.rollup($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      ctx.sql("select a + b, b, sum(a - b) from mytable group by a + b, b with rollup").collect()
    )

    checkAnswer(
      _testData.rollup("a", "b").agg(sum("b")),
      ctx.sql("select a, b, sum(b) from mytable group by a, b with rollup").collect()
    )
  }

  test("cube") {
    checkAnswer(
      _testData.cube($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      ctx.sql("select a + b, b, sum(a - b) from mytable group by a + b, b with cube").collect()
    )

    checkAnswer(
      _testData.cube("a", "b").agg(sum("b")),
      ctx.sql("select a, b, sum(b) from mytable group by a, b with cube").collect()
    )
  }
}
