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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.types.{DataTypes, MapType}

class HiveResultSuite extends SharedSparkSession {
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

  test("decimal formatting in hive result") {
    val df = Seq(new java.math.BigDecimal("1")).toDS()
    Seq(2, 6, 18).foreach { scala =>
      val executedPlan =
        df.selectExpr(s"CAST(value AS decimal(38, $scala))").queryExecution.executedPlan
      val result = HiveResult.hiveResultString(executedPlan)
      assert(result.head.split("\\.").last.length === scala)
    }

    val executedPlan = Seq(java.math.BigDecimal.ZERO).toDS()
      .selectExpr(s"CAST(value AS decimal(38, 8))").queryExecution.executedPlan
    val result = HiveResult.hiveResultString(executedPlan)
    assert(result.head === "0.00000000")
  }

  test("SPARK-26544: toHiveString correctly escape map type") {
    withSQLConf(SQLConf.THRIFTSERVER_RESULT_ESCAPE_STRUCT_STRING.key -> "true") {
      val m = Map("log_pb" -> """{"impr_id":"20181231"}""", "request_id" -> "001")
      val mapType = new MapType(DataTypes.StringType, DataTypes.StringType, true)
      assert(HiveResult.toHiveString((m, mapType)) ===
        """{"log_pb":"{\"impr_id\":\"20181231\"}","request_id":"001"}""")
    }
  }

  test("SPARK-26544: toHiveString correctly escape map type with specially characters") {
    withSQLConf(SQLConf.THRIFTSERVER_RESULT_ESCAPE_STRUCT_STRING.key -> "true") {
      val specialChars = List(8, 9, 10, 13, 6).map(_.toChar).mkString("") // \b\t\n\r\u0006
      val m = Map("log_pb" -> s"""{"impr_id":"special chars $specialChars"}""")
      val mapType = new MapType(DataTypes.StringType, DataTypes.StringType, true)
      val result = HiveResult.toHiveString((m, mapType))

      val c1 = Array("\\", "b").mkString("")
      val c2 = Array("\\", "t").mkString("")
      val c3 = Array("\\", "n").mkString("")
      val c4 = Array("\\", "r").mkString("")
      val c5 = Array("\\", "u", "0", "0", "0", "6").mkString("")
      val expected =
        """{"log_pb":"{\"impr_id\":\"special chars """ + c1 + c2 + c3 + c4 + c5 + """\"}"}"""
      assert(result === expected)
    }
  }
}
