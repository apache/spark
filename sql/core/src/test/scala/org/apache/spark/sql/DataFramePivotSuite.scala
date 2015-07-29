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

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.functions._

class DataFramePivotSuite extends QueryTest {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("pivot courses") {
    checkAnswer(
      courseSales.pivot(Seq($"year"), $"course", Seq("dotNET", "Java"), sum($"earnings")),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }

  test("pivot year") {
    checkAnswer(
      courseSales.pivot(Seq($"course"), $"year", Seq("2012", "2013"), sum($"earnings")),
      Row("dotNet", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

}
