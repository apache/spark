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

import org.apache.spark.sql.catalyst.DefinedByConstructorParams
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

class S100(
  val s1: String = "1", val s2: String = "2", val s3: String = "3", val s4: String = "4",
  val s5: String = "5", val s6: String = "6", val s7: String = "7", val s8: String = "8",
  val s9: String = "9", val s10: String = "10", val s11: String = "11", val s12: String = "12",
  val s13: String = "13", val s14: String = "14", val s15: String = "15", val s16: String = "16",
  val s17: String = "17", val s18: String = "18", val s19: String = "19", val s20: String = "20",
  val s21: String = "21", val s22: String = "22", val s23: String = "23", val s24: String = "24",
  val s25: String = "25", val s26: String = "26", val s27: String = "27", val s28: String = "28",
  val s29: String = "29", val s30: String = "30", val s31: String = "31", val s32: String = "32",
  val s33: String = "33", val s34: String = "34", val s35: String = "35", val s36: String = "36",
  val s37: String = "37", val s38: String = "38", val s39: String = "39", val s40: String = "40",
  val s41: String = "41", val s42: String = "42", val s43: String = "43", val s44: String = "44",
  val s45: String = "45", val s46: String = "46", val s47: String = "47", val s48: String = "48",
  val s49: String = "49", val s50: String = "50", val s51: String = "51", val s52: String = "52",
  val s53: String = "53", val s54: String = "54", val s55: String = "55", val s56: String = "56",
  val s57: String = "57", val s58: String = "58", val s59: String = "59", val s60: String = "60",
  val s61: String = "61", val s62: String = "62", val s63: String = "63", val s64: String = "64",
  val s65: String = "65", val s66: String = "66", val s67: String = "67", val s68: String = "68",
  val s69: String = "69", val s70: String = "70", val s71: String = "71", val s72: String = "72",
  val s73: String = "73", val s74: String = "74", val s75: String = "75", val s76: String = "76",
  val s77: String = "77", val s78: String = "78", val s79: String = "79", val s80: String = "80",
  val s81: String = "81", val s82: String = "82", val s83: String = "83", val s84: String = "84",
  val s85: String = "85", val s86: String = "86", val s87: String = "87", val s88: String = "88",
  val s89: String = "89", val s90: String = "90", val s91: String = "91", val s92: String = "92",
  val s93: String = "93", val s94: String = "94", val s95: String = "95", val s96: String = "96",
  val s97: String = "97", val s98: String = "98", val s99: String = "99", val s100: String = "100")
extends DefinedByConstructorParams

case class S100_5(
  s1: S100 = new S100(), s2: S100 = new S100(), s3: S100 = new S100(),
  s4: S100 = new S100(), s5: S100 = new S100())


