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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSQLContext}

class HiveResultSuite extends SparkFunSuite with SharedSQLContext {
  import testImplicits._

  test("date formatting in hive result") {
    val date = "2018-12-28"
    val executedPlan = Seq(Date.valueOf(date)).toDS().queryExecution.executedPlan
    val result = HiveResult.hiveResultString(executedPlan)
    assert(result.head == date)
  }

  test("timestamp formatting in hive result") {
    val timestamp = "2018-12-28 01:02:03"
    val executedPlan = Seq(Timestamp.valueOf(timestamp)).toDS().queryExecution.executedPlan
    val result = HiveResult.hiveResultString(executedPlan)
    assert(result.head == timestamp)
  }

  test("toHiveString correctly handles UDTs") {
    val point = new ExamplePoint(50.0, 50.0)
    val tpe = new ExamplePointUDT()
    assert(HiveResult.toHiveString((point, tpe)) === "(50.0, 50.0)")
  }
}
