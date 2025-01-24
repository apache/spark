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
package org.apache.spark.sql.connect

import java.util.Collections

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Column
import org.apache.spark.sql.avro.{functions => avroFn}
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Tests for client local function behavior.
 *
 * This mostly tests is various function variants produce the same columns.
 */
class FunctionTestSuite extends ConnectFunSuite {
  private def testEquals(name: String, columns: Column*): Unit = {
    test(name) {
      assert(columns.nonEmpty)
      val unique = columns.distinct
      assert(unique.size == 1)
    }
  }

  private val a = col("a")
  private val b = col("b")
  private val c = col("c")

  private val schema = new StructType()
    .add("key", "long")
    .add("value", "string")

  testEquals("col/column", a, column("a"))
  testEquals("asc/asc_nulls_first", asc("a"), asc_nulls_first("a"))
  testEquals("desc/desc_nulls_last", desc("a"), desc_nulls_last("a"))
  testEquals(
    "approx_count_distinct",
    approxCountDistinct(a),
    approxCountDistinct("a"),
    approx_count_distinct("a"),
    approx_count_distinct(a))
  testEquals(
    "approx_count_distinct rsd",
    approxCountDistinct(a, 0.1),
    approxCountDistinct("a", 0.1),
    approx_count_distinct("a", 0.1),
    approx_count_distinct(a, 0.1))
  testEquals("avg/mean", avg("a"), avg(a), mean(a), mean("a"))
  testEquals("collect_list", collect_list("a"), collect_list(a))
  testEquals("collect_set", collect_set("a"), collect_set(a))
  testEquals("corr", corr("a", "b"), corr(a, b))
  testEquals(
    "count_distinct",
    countDistinct(a, b, c),
    countDistinct("a", "b", "c"),
    count_distinct(a, b, c))
  testEquals("covar_pop", covar_pop(a, b), covar_pop("a", "b"))
  testEquals("covar_samp", covar_samp(a, b), covar_samp("a", "b"))
  testEquals(
    "first",
    first("a"),
    first(a),
    first("a", ignoreNulls = false),
    first(a, ignoreNulls = false))
  testEquals("grouping", grouping("a"), grouping(a))
  testEquals("grouping_id", grouping_id("a", "b"), grouping_id(a, b))
  testEquals("kurtosis", kurtosis("a"), kurtosis(a))
  testEquals(
    "last",
    last("a"),
    last(a),
    last("a", ignoreNulls = false),
    last(a, ignoreNulls = false))
  testEquals("max", max("a"), max(a))
  testEquals("min", min("a"), min(a))
  testEquals("skewness", skewness("a"), skewness(a))
  testEquals("stddev", stddev("a"), stddev(a))
  testEquals("stddev_samp", stddev_samp("a"), stddev_samp(a))
  testEquals("stddev_pop", stddev_pop("a"), stddev_pop(a))
  testEquals("sum", sum("a"), sum(a))
  testEquals("sum_distinct", sumDistinct("a"), sumDistinct(a), sum_distinct(a))
  testEquals("variance", variance("a"), variance(a))
  testEquals("var_samp", var_samp("a"), var_samp(a))
  testEquals("var_pop", var_pop("a"), var_pop(a))
  testEquals("array", array(a, b, c), array("a", "b", "c"))
  testEquals(
    "monotonicallyIncreasingId",
    monotonicallyIncreasingId(),
    monotonically_increasing_id())
  testEquals("sqrt", sqrt("a"), sqrt(a))
  testEquals("struct", struct(a, c, b), struct("a", "c", "b"))
  testEquals("bitwise_not", bitwiseNOT(a), bitwise_not(a))
  testEquals("acos", acos("a"), acos(a))
  testEquals("acosh", acosh("a"), acosh(a))
  testEquals("asin", asin("a"), asin(a))
  testEquals("asinh", asinh("a"), asinh(a))
  testEquals("atan", atan("a"), atan(a))
  testEquals("atan2", atan2(a, b), atan2(a, "b"), atan2("a", b), atan2("a", "b"))
  testEquals("atanh", atanh("a"), atanh(a))
  testEquals("bin", bin("a"), bin(a))
  testEquals("cbrt", cbrt("a"), cbrt(a))
  testEquals("ceil", ceil(a), ceil("a"))
  testEquals("cos", cos("a"), cos(a))
  testEquals("cosh", cosh("a"), cosh(a))
  testEquals("exp", exp("a"), exp(a))
  testEquals("expm1", expm1("a"), expm1(a))
  testEquals("floor", floor(a), floor("a"))
  testEquals("greatest", greatest(a, b, c), greatest("a", "b", "c"))
  testEquals("hypot", hypot(a, b), hypot("a", b), hypot(a, "b"), hypot("a", "b"))
  testEquals(
    "hypot right fixed",
    hypot(lit(3d), a),
    hypot(lit(3d), "a"),
    hypot(3d, a),
    hypot(3d, "a"))
  testEquals(
    "hypot left fixed",
    hypot(a, lit(4d)),
    hypot(a, 4d),
    hypot("a", lit(4d)),
    hypot("a", 4d))
  testEquals("least", least(a, b, c), least("a", "b", "c"))
  testEquals("log", log("a"), log(a))
  testEquals("log base", log(2.0, "a"), log(2.0, a))
  testEquals("log10", log10("a"), log10(a))
  testEquals("log1p", log1p("a"), log1p(a))
  testEquals("log2", log2("a"), log2(a))
  testEquals("pow", pow(a, b), pow(a, "b"), pow("a", b), pow("a", "b"))
  testEquals("pow left fixed", pow(lit(7d), b), pow(lit(7d), "b"), pow(7d, b), pow(7d, "b"))
  testEquals("pow right fixed", pow(a, lit(9d)), pow(a, 9d), pow("a", lit(9d)), pow("a", 9d))
  testEquals("rint", rint(a), rint("a"))
  testEquals("round", round(a), round(a, 0))
  testEquals("bround", bround(a), bround(a, 0))
  testEquals("shiftleft", shiftLeft(a, 2), shiftleft(a, 2))
  testEquals("shiftright", shiftRight(a, 3), shiftright(a, 3))
  testEquals("shiftrightunsigned", shiftRightUnsigned(a, 3), shiftrightunsigned(a, 3))
  testEquals("signum", signum("a"), signum(a))
  testEquals("sin", sin("a"), sin(a))
  testEquals("sinh", sinh("a"), sinh(a))
  testEquals("tan", tan("a"), tan(a))
  testEquals("tanh", tanh("a"), tanh(a))
  testEquals("degrees", toDegrees(a), toDegrees("a"), degrees(a), degrees("a"))
  testEquals("radians", toRadians(a), toRadians("a"), radians(a), radians("a"))
  testEquals(
    "regexp_replace",
    regexp_replace(a, lit("foo"), lit("bar")),
    regexp_replace(a, "foo", "bar"))
  testEquals("add_months", add_months(a, lit(1)), add_months(a, 1))
  testEquals("date_add", date_add(a, lit(2)), date_add(a, 2))
  testEquals("date_sub", date_sub(a, lit(2)), date_sub(a, 2))
  testEquals("next_day", next_day(a, lit("Mon")), next_day(a, lit("Mon")))
  testEquals("unix_timestamp", unix_timestamp(), unix_timestamp(current_timestamp()))
  testEquals(
    "from_utc_timestamp",
    from_utc_timestamp(a, "GMT"),
    from_utc_timestamp(a, lit("GMT")))
  testEquals("to_utc_timestamp", to_utc_timestamp(a, "GMT"), to_utc_timestamp(a, lit("GMT")))
  testEquals(
    "window",
    window(a, "10 seconds", "10 seconds", "0 second"),
    window(a, "10 seconds", "10 seconds"),
    window(a, "10 seconds"))
  testEquals("session_window", session_window(a, "1 second"), session_window(a, lit("1 second")))
  testEquals("slice", slice(a, 1, 2), slice(a, lit(1), lit(2)))
  testEquals("bucket", bucket(lit(3), a), bucket(3, a))
  testEquals(
    "lag",
    lag(a, 1),
    lag("a", 1),
    lag(a, 1, null),
    lag("a", 1, null),
    lag(a, 1, null, false))
  testEquals(
    "lead",
    lead(a, 2),
    lead("a", 2),
    lead(a, 2, null),
    lead("a", 2, null),
    lead(a, 2, null, false))
  testEquals(
    "from_json with sql schema",
    from_json(a, schema.asInstanceOf[DataType]),
    from_json(a, schema),
    from_json(a, schema.asInstanceOf[DataType], Map.empty[String, String]),
    from_json(a, schema.asInstanceOf[DataType], Collections.emptyMap[String, String]),
    from_json(a, schema, Map.empty[String, String]),
    from_json(a, schema, Collections.emptyMap[String, String]))
  testEquals(
    "from_json with json schema",
    from_json(a, lit(schema.json)),
    from_json(a, schema.json, Map.empty[String, String]),
    from_json(a, schema.json, Collections.emptyMap[String, String]),
    from_json(a, lit(schema.json), Collections.emptyMap[String, String]))
  testEquals("schema_of_json", schema_of_json(lit("x,y")), schema_of_json("x,y"))
  testEquals(
    "to_json",
    to_json(a),
    to_json(a, Collections.emptyMap[String, String]),
    to_json(a, Map.empty[String, String]))
  testEquals("sort_array", sort_array(a), sort_array(a, asc = true))
  testEquals(
    "from_csv",
    from_csv(a, lit(schema.toDDL), Collections.emptyMap[String, String]),
    from_csv(a, schema, Map.empty[String, String]))
  testEquals(
    "schema_of_csv",
    schema_of_csv(lit("x,y")),
    schema_of_csv("x,y"),
    schema_of_csv(lit("x,y"), Collections.emptyMap()))
  testEquals("to_csv", to_csv(a), to_csv(a, Collections.emptyMap[String, String]))
  testEquals(
    "from_xml with sql schema",
    from_xml(a, schema),
    from_xml(a, schema, Map.empty[String, String].asJava),
    from_xml(a, schema, Collections.emptyMap[String, String]))
  testEquals(
    "from_xml with json schema",
    from_xml(a, lit(schema.json)),
    from_xml(a, schema.json, Collections.emptyMap[String, String]),
    from_xml(a, schema.json, Map.empty[String, String].asJava),
    from_xml(a, lit(schema.json), Collections.emptyMap[String, String]))
  testEquals(
    "schema_of_xml",
    schema_of_xml(lit("<p><a>1.0</a><b>test</b></p>")),
    schema_of_xml("<p><a>1.0</a><b>test</b></p>"),
    schema_of_xml(lit("<p><a>1.0</a><b>test</b></p>"), Collections.emptyMap()))
  testEquals("to_xml", to_xml(a), to_xml(a, Collections.emptyMap[String, String]))

  testEquals(
    "from_avro",
    avroFn.from_avro(a, """{"type": "int", "name": "id"}"""),
    avroFn.from_avro(
      a,
      """{"type": "int", "name": "id"}""",
      Collections.emptyMap[String, String]))

  testEquals("call_udf", callUDF("bob", lit(1)), call_udf("bob", lit(1)))

  test("assert_true no message") {
    val e = toExpr(assert_true(a))
    assert(e.hasUnresolvedFunction)
    val fn = e.getUnresolvedFunction
    assert(fn.getFunctionName == "assert_true")
    assert(fn.getArgumentsCount == 1)
    assert(fn.getArguments(0) == toExpr(a))
  }

  test("json_tuple zero args") {
    intercept[IllegalArgumentException](json_tuple(a))
  }

  test("rand no seed") {
    val e = toExpr(rand())
    assert(e.hasUnresolvedFunction)
    val fn = e.getUnresolvedFunction
    assert(fn.getFunctionName == "rand")
    assert(fn.getArgumentsCount == 1)
  }

  test("randn no seed") {
    val e = toExpr(randn())
    assert(e.hasUnresolvedFunction)
    val fn = e.getUnresolvedFunction
    assert(fn.getFunctionName == "randn")
    assert(fn.getArgumentsCount == 1)
  }
}
