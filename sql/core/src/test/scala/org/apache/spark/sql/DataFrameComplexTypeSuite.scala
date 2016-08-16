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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

/**
 * A test suite to test DataFrame/SQL functionalities with complex types (i.e. array, struct, map).
 */
class DataFrameComplexTypeSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("UDF on struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(struct($"a").as("s")).select(f($"s.a")).collect()
  }

  test("UDF on named_struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.selectExpr("named_struct('a', a) s").select(f($"s.a")).collect()
  }

  test("UDF on array") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(array($"a").as("s")).select(f($"s".getItem(0))).collect()
  }

  test("UDF on map") {
    val f = udf((a: String) => a)
    val df = Seq("a" -> 1).toDF("a", "b")
    df.select(map($"a", $"b").as("s")).select(f($"s".getItem("a"))).collect()
  }

  test("SPARK-12477 accessing null element in array field") {
    val df = sparkContext.parallelize(Seq((Seq("val1", null, "val2"),
      Seq(Some(1), None, Some(2))))).toDF("s", "i")
    val nullStringRow = df.selectExpr("s[1]").collect()(0)
    assert(nullStringRow == org.apache.spark.sql.Row(null))
    val nullIntRow = df.selectExpr("i[1]").collect()(0)
    assert(nullIntRow == org.apache.spark.sql.Row(null))
  }

  test("SPARK-15285 Generated SpecificSafeProjection.apply method grows beyond 64KB") {
    val ds100_5 = Seq(S100_5()).toDS()
    ds100_5.rdd.count
  }
}

case class S100(
  s1: String = "1", s2: String = "2", s3: String = "3", s4: String = "4",
  s5: String = "5", s6: String = "6", s7: String = "7", s8: String = "8",
  s9: String = "9", s10: String = "10", s11: String = "11", s12: String = "12",
  s13: String = "13", s14: String = "14", s15: String = "15", s16: String = "16",
  s17: String = "17", s18: String = "18", s19: String = "19", s20: String = "20",
  s21: String = "21", s22: String = "22", s23: String = "23", s24: String = "24",
  s25: String = "25", s26: String = "26", s27: String = "27", s28: String = "28",
  s29: String = "29", s30: String = "30", s31: String = "31", s32: String = "32",
  s33: String = "33", s34: String = "34", s35: String = "35", s36: String = "36",
  s37: String = "37", s38: String = "38", s39: String = "39", s40: String = "40",
  s41: String = "41", s42: String = "42", s43: String = "43", s44: String = "44",
  s45: String = "45", s46: String = "46", s47: String = "47", s48: String = "48",
  s49: String = "49", s50: String = "50", s51: String = "51", s52: String = "52",
  s53: String = "53", s54: String = "54", s55: String = "55", s56: String = "56",
  s57: String = "57", s58: String = "58", s59: String = "59", s60: String = "60",
  s61: String = "61", s62: String = "62", s63: String = "63", s64: String = "64",
  s65: String = "65", s66: String = "66", s67: String = "67", s68: String = "68",
  s69: String = "69", s70: String = "70", s71: String = "71", s72: String = "72",
  s73: String = "73", s74: String = "74", s75: String = "75", s76: String = "76",
  s77: String = "77", s78: String = "78", s79: String = "79", s80: String = "80",
  s81: String = "81", s82: String = "82", s83: String = "83", s84: String = "84",
  s85: String = "85", s86: String = "86", s87: String = "87", s88: String = "88",
  s89: String = "89", s90: String = "90", s91: String = "91", s92: String = "92",
  s93: String = "93", s94: String = "94", s95: String = "95", s96: String = "96",
  s97: String = "97", s98: String = "98", s99: String = "99", s100: String = "100")

case class S100_5(
  s1: S100 = S100(), s2: S100 = S100(), s3: S100 = S100(), s4: S100 = S100(), s5: S100 = S100())
