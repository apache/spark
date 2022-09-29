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

package org.apache.spark.sql.catalyst.expressions

import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.util.{Calendar, Locale, TimeZone}

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{PST, UTC, UTC_OPT}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class JsonExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper with PlanTestBase {
  val json =
    """
      |{"store":{"fruit":[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}],
      |"basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],"book":[{"author":"Nigel Rees",
      |"title":"Sayings of the Century","category":"reference","price":8.95},
      |{"author":"Herman Melville","title":"Moby Dick","category":"fiction","price":8.99,
      |"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":"The Lord of the Rings",
      |"category":"fiction","reader":[{"age":25,"name":"bob"},{"age":26,"name":"jack"}],
      |"price":22.99,"isbn":"0-395-19395-8"}],"bicycle":{"price":19.95,"color":"red"}},
      |"email":"amy@only_for_json_udf_test.net","owner":"amy","zip code":"94025",
      |"fb:testid":"1234"}
      |""".stripMargin

  /* invalid json with leading nulls would trigger java.io.CharConversionException
   in Jackson's JsonFactory.createParser(byte[]) due to RFC-4627 encoding detection */
  val badJson = "\u0000\u0000\u0000A\u0001AAA"

  test("get_json_object escaping") {
    GenerateUnsafeProjection.generate(GetJsonObject(Literal("\"quote"), Literal("\"quote")) :: Nil)
  }

  test("$.store.bicycle") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.bicycle")),
      """{"price":19.95,"color":"red"}""")
  }

  test("$['store'].bicycle") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$['store'].bicycle")),
      """{"price":19.95,"color":"red"}""")
  }

  test("$.store['bicycle']") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store['bicycle']")),
      """{"price":19.95,"color":"red"}""")
  }

  test("$['store']['bicycle']") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$['store']['bicycle']")),
      """{"price":19.95,"color":"red"}""")
  }

  test("$['key with spaces']") {
    checkEvaluation(GetJsonObject(
      Literal("""{ "key with spaces": "it works" }"""), Literal("$['key with spaces']")),
      "it works")
  }

  test("$.store.book") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book")),
      """[{"author":"Nigel Rees","title":"Sayings of the Century","category":"reference",
        |"price":8.95},{"author":"Herman Melville","title":"Moby Dick","category":"fiction",
        |"price":8.99,"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":
        |"The Lord of the Rings","category":"fiction","reader":[{"age":25,"name":"bob"},
        |{"age":26,"name":"jack"}],"price":22.99,"isbn":"0-395-19395-8"}]
        |""".stripMargin.replace("\n", ""))
  }

  test("$.store.book[0]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[0]")),
      """{"author":"Nigel Rees","title":"Sayings of the Century",
        |"category":"reference","price":8.95}""".stripMargin.replace("\n", ""))
  }

  test("$.store.book[*]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[*]")),
      """[{"author":"Nigel Rees","title":"Sayings of the Century","category":"reference",
        |"price":8.95},{"author":"Herman Melville","title":"Moby Dick","category":"fiction",
        |"price":8.99,"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":
        |"The Lord of the Rings","category":"fiction","reader":[{"age":25,"name":"bob"},
        |{"age":26,"name":"jack"}],"price":22.99,"isbn":"0-395-19395-8"}]
        |""".stripMargin.replace("\n", ""))
  }

  test("$") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$")),
      json.replace("\n", ""))
  }

  test("$.store.book[0].category") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[0].category")),
      "reference")
  }

  test("$.store.book[*].category") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[*].category")),
      """["reference","fiction","fiction"]""")
  }

  test("$.store.book[*].isbn") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[*].isbn")),
      """["0-553-21311-3","0-395-19395-8"]""")
  }

  test("$.store.book[*].reader") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[*].reader")),
      """[{"age":25,"name":"bob"},{"age":26,"name":"jack"}]""")
  }

  test("$.store.basket[0][1]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[0][1]")),
      "2")
  }

  test("$.store.basket[*]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[*]")),
      """[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]]""")
  }

  test("$.store.basket[*][0]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[*][0]")),
      "[1,3,5]")
  }

  test("$.store.basket[0][*]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[0][*]")),
      """[1,2,{"b":"y","a":"x"}]""")
  }

  test("$.store.basket[*][*]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[*][*]")),
      """[1,2,{"b":"y","a":"x"},3,4,5,6]""")
  }

  test("$.store.basket[0][2].b") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[0][2].b")),
      "y")
  }

  test("$.store.basket[0][*].b") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[0][*].b")),
      """["y"]""")
  }

  test("$.zip code") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.zip code")),
      "94025")
  }

  test("$.fb:testid") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.fb:testid")),
      "1234")
  }

  test("preserve newlines") {
    checkEvaluation(
      GetJsonObject(Literal("""{"a":"b\nc"}"""), Literal("$.a")),
      "b\nc")
  }

  test("escape") {
    checkEvaluation(
      GetJsonObject(Literal("""{"a":"b\"c"}"""), Literal("$.a")),
      "b\"c")
  }

  test("$.non_exist_key") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.non_exist_key")),
      null)
  }

  test("$..no_recursive") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$..no_recursive")),
      null)
  }

  test("$.store.book[10]") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[10]")),
      null)
  }

  test("$.store.book[0].non_exist_key") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.book[0].non_exist_key")),
      null)
  }

  test("$.store.basket[*].non_exist_key") {
    checkEvaluation(
      GetJsonObject(Literal(json), Literal("$.store.basket[*].non_exist_key")),
      null)
  }

  test("SPARK-16548: character conversion") {
    checkEvaluation(
      GetJsonObject(Literal(badJson), Literal("$.a")),
      null
    )
  }

  test("non foldable literal") {
    checkEvaluation(
      GetJsonObject(NonFoldableLiteral(json), NonFoldableLiteral("$.fb:testid")),
      "1234")
  }

  test("some big value") {
    val value = "x" * 3000
    checkEvaluation(
      GetJsonObject(NonFoldableLiteral((s"""{"big": "$value"}""")),
      NonFoldableLiteral("$.big")), value)
  }

  val jsonTupleQuery = Literal("f1") ::
    Literal("f2") ::
    Literal("f3") ::
    Literal("f4") ::
    Literal("f5") ::
    Nil

  private def checkJsonTuple(jt: JsonTuple, expected: InternalRow): Unit = {
    assert(jt.eval(null).toSeq.head === expected)
  }

  test("json_tuple escaping") {
    GenerateUnsafeProjection.generate(
      JsonTuple(Literal("\"quote") ::  Literal("\"quote") :: Nil) :: Nil)
  }

  test("json_tuple - hive key 1") {
    checkJsonTuple(
      JsonTuple(
        Literal("""{"f1": "value1", "f2": "value2", "f3": 3, "f5": 5.23}""") ::
          jsonTupleQuery),
      InternalRow.fromSeq(Seq("value1", "value2", "3", null, "5.23").map(UTF8String.fromString)))
  }

  test("json_tuple - hive key 2") {
    checkJsonTuple(
      JsonTuple(
        Literal("""{"f1": "value12", "f3": "value3", "f2": 2, "f4": 4.01}""") ::
          jsonTupleQuery),
      InternalRow.fromSeq(Seq("value12", "2", "value3", "4.01", null).map(UTF8String.fromString)))
  }

  test("json_tuple - hive key 2 (mix of foldable fields)") {
    checkJsonTuple(
      JsonTuple(Literal("""{"f1": "value12", "f3": "value3", "f2": 2, "f4": 4.01}""") ::
        Literal("f1") ::
        NonFoldableLiteral("f2") ::
        NonFoldableLiteral("f3") ::
        Literal("f4") ::
        Literal("f5") ::
        Nil),
      InternalRow.fromSeq(Seq("value12", "2", "value3", "4.01", null).map(UTF8String.fromString)))
  }

  test("json_tuple - hive key 3") {
    checkJsonTuple(
      JsonTuple(
        Literal("""{"f1": "value13", "f4": "value44", "f3": "value33", "f2": 2, "f5": 5.01}""") ::
          jsonTupleQuery),
      InternalRow.fromSeq(
        Seq("value13", "2", "value33", "value44", "5.01").map(UTF8String.fromString)))
  }

  test("json_tuple - hive key 3 (nonfoldable json)") {
    checkJsonTuple(
      JsonTuple(
        NonFoldableLiteral(
          """{"f1": "value13", "f4": "value44",
            | "f3": "value33", "f2": 2, "f5": 5.01}""".stripMargin)
          :: jsonTupleQuery),
      InternalRow.fromSeq(
        Seq("value13", "2", "value33", "value44", "5.01").map(UTF8String.fromString)))
  }

  test("json_tuple - hive key 3 (nonfoldable fields)") {
    checkJsonTuple(
      JsonTuple(Literal(
        """{"f1": "value13", "f4": "value44",
          | "f3": "value33", "f2": 2, "f5": 5.01}""".stripMargin) ::
        NonFoldableLiteral("f1") ::
        NonFoldableLiteral("f2") ::
        NonFoldableLiteral("f3") ::
        NonFoldableLiteral("f4") ::
        NonFoldableLiteral("f5") ::
        Nil),
      InternalRow.fromSeq(
        Seq("value13", "2", "value33", "value44", "5.01").map(UTF8String.fromString)))
  }

  test("json_tuple - hive key 4 - null json") {
    checkJsonTuple(
      JsonTuple(Literal(null) :: jsonTupleQuery),
      InternalRow(null, null, null, null, null))
  }

  test("json_tuple - hive key 5 - null and empty fields") {
    checkJsonTuple(
      JsonTuple(Literal("""{"f1": "", "f5": null}""") :: jsonTupleQuery),
      InternalRow(UTF8String.fromString(""), null, null, null, null))
  }

  test("json_tuple - hive key 6 - invalid json (array)") {
    checkJsonTuple(
      JsonTuple(Literal("[invalid JSON string]") :: jsonTupleQuery),
      InternalRow(null, null, null, null, null))
  }

  test("json_tuple - invalid json (object start only)") {
    checkJsonTuple(
      JsonTuple(Literal("{") :: jsonTupleQuery),
      InternalRow(null, null, null, null, null))
  }

  test("json_tuple - invalid json (no object end)") {
    checkJsonTuple(
      JsonTuple(Literal("""{"foo": "bar"""") :: jsonTupleQuery),
      InternalRow(null, null, null, null, null))
  }

  test("json_tuple - invalid json (invalid json)") {
    checkJsonTuple(
      JsonTuple(Literal("\\") :: jsonTupleQuery),
      InternalRow(null, null, null, null, null))
  }

  test("SPARK-16548: json_tuple - invalid json with leading nulls") {
    checkJsonTuple(
      JsonTuple(Literal(badJson) :: jsonTupleQuery),
      InternalRow(null, null, null, null, null))
  }

  test("json_tuple - preserve newlines") {
    checkJsonTuple(
      JsonTuple(Literal("{\"a\":\"b\nc\"}") :: Literal("a") :: Nil),
      InternalRow(UTF8String.fromString("b\nc")))
  }

  test("SPARK-21677: json_tuple throws NullPointException when column is null as string type") {
    checkJsonTuple(
      JsonTuple(Literal("""{"f1": 1, "f2": 2}""") ::
        NonFoldableLiteral("f1") ::
        NonFoldableLiteral("cast(NULL AS STRING)") ::
        NonFoldableLiteral("f2") ::
        Nil),
      InternalRow(UTF8String.fromString("1"), null, UTF8String.fromString("2")))
  }

  test("SPARK-21804: json_tuple returns null values within repeated columns except the first one") {
    checkJsonTuple(
      JsonTuple(Literal("""{"f1": 1, "f2": 2}""") ::
        NonFoldableLiteral("f1") ::
        NonFoldableLiteral("cast(NULL AS STRING)") ::
        NonFoldableLiteral("f1") ::
        Nil),
      InternalRow(UTF8String.fromString("1"), null, UTF8String.fromString("1")))
  }

  test("from_json escaping") {
    val schema = StructType(StructField("\"quote", IntegerType) :: Nil)
    GenerateUnsafeProjection.generate(
      JsonToStructs(schema, Map.empty, Literal("\"quote"), UTC_OPT) :: Nil)
  }

  test("from_json") {
    val jsonData = """{"a": 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData), UTC_OPT),
      InternalRow(1)
    )
  }

  test("from_json - invalid data") {
    val jsonData = """{"a" 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData), UTC_OPT),
      InternalRow(null)
    )

    val exception = intercept[TestFailedException] {
      checkEvaluation(
        JsonToStructs(schema, Map("mode" -> FailFastMode.name), Literal(jsonData), UTC_OPT),
        InternalRow(null)
      )
    }.getCause
    assert(exception.isInstanceOf[SparkException])
    assert(exception.getMessage.contains(
      "Malformed records are detected in record parsing. Parse Mode: FAILFAST"))
  }

  test("from_json - input=array, schema=array, output=array") {
    val input = """[{"a": 1}, {"a": 2}]"""
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = InternalRow(1) :: InternalRow(2) :: Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=object, schema=array, output=array of single row") {
    val input = """{"a": 1}"""
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = InternalRow(1) :: Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=empty array, schema=array, output=empty array") {
    val input = "[ ]"
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=empty object, schema=array, output=array of single row with null") {
    val input = "{ }"
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = InternalRow(null) :: Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=array of single object, schema=struct, output=single row") {
    val input = """[{"a": 1}]"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(null)
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=array, schema=struct, output=single row") {
    val input = """[{"a": 1}, {"a": 2}]"""
    val corrupted = "corrupted"
    val schema = new StructType().add("a", IntegerType).add(corrupted, StringType)
    val output = InternalRow(null, UTF8String.fromString(input))
    val options = Map("columnNameOfCorruptRecord" -> corrupted)
    checkEvaluation(JsonToStructs(schema, options, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=empty array, schema=struct, output=single row with null") {
    val input = """[]"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(null)
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json - input=empty object, schema=struct, output=single row with null") {
    val input = """{  }"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(null)
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_json null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal.create(null, StringType), UTC_OPT),
      null
    )
  }

  test("SPARK-20549: from_json bad UTF-8") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(badJson), UTC_OPT),
      InternalRow(null))
  }

  test("from_json with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)

    val jsonData1 = """{"t": "2016-01-01T00:00:00.123Z"}"""
    var c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 123)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData1), UTC_OPT),
      InternalRow(c.getTimeInMillis * 1000L)
    )
    // The result doesn't change because the json string includes timezone string ("Z" here),
    // which means the string represents the timestamp string in the timezone regardless of
    // the timeZoneId parameter.
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData1), Option(PST.getId)),
      InternalRow(c.getTimeInMillis * 1000L)
    )

    val jsonData2 = """{"t": "2016-01-01T00:00:00"}"""
    for (zid <- DateTimeTestUtils.outstandingZoneIds) {
      c = Calendar.getInstance(TimeZone.getTimeZone(zid))
      c.set(2016, 0, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkEvaluation(
        JsonToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"),
          Literal(jsonData2),
          Option(zid.getId)),
        InternalRow(c.getTimeInMillis * 1000L)
      )
      checkEvaluation(
        JsonToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
            DateTimeUtils.TIMEZONE_OPTION -> zid.getId),
          Literal(jsonData2),
          UTC_OPT),
        InternalRow(c.getTimeInMillis * 1000L)
      )
    }
  }

  test("SPARK-19543: from_json empty input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal.create(" ", StringType), UTC_OPT),
      null
    )
  }

  test("to_json escaping") {
    val schema = StructType(StructField("\"quote", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    GenerateUnsafeProjection.generate(
      StructsToJson(Map.empty, struct, UTC_OPT) :: Nil)
  }

  test("to_json - struct") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    checkEvaluation(
      StructsToJson(Map.empty, struct, UTC_OPT),
      """{"a":1}"""
    )
  }

  test("to_json - array") {
    val inputSchema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val input = new GenericArrayData(InternalRow(1) :: InternalRow(2) :: Nil)
    val output = """[{"a":1},{"a":2}]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_json - array with single empty row") {
    val inputSchema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val input = new GenericArrayData(InternalRow(null) :: Nil)
    val output = """[{}]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_json - empty array") {
    val inputSchema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val input = new GenericArrayData(Nil)
    val output = """[]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_json null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(null, schema)
    checkEvaluation(
      StructsToJson(Map.empty, struct, UTC_OPT),
      null
    )
  }

  test("to_json with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)
    val c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    val struct = Literal.create(create_row(c.getTimeInMillis * 1000L), schema)

    checkEvaluation(
      StructsToJson(Map.empty, struct, UTC_OPT),
      """{"t":"2016-01-01T00:00:00.000Z"}"""
    )
    checkEvaluation(
      StructsToJson(Map.empty, struct, Option(PST.getId)),
      """{"t":"2015-12-31T16:00:00.000-08:00"}"""
    )

    checkEvaluation(
      StructsToJson(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> UTC_OPT.get),
        struct,
        UTC_OPT),
      """{"t":"2016-01-01T00:00:00"}"""
    )
    checkEvaluation(
      StructsToJson(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> PST.getId),
        struct,
        UTC_OPT),
      """{"t":"2015-12-31T16:00:00"}"""
    )
  }

  test("SPARK-21513: to_json support map[string, struct] to json") {
    val schema = MapType(StringType, StructType(StructField("a", IntegerType) :: Nil))
    val input = Literal(
      ArrayBasedMapData(Map(UTF8String.fromString("test") -> InternalRow(1))), schema)
    checkEvaluation(
      StructsToJson(Map.empty, input),
      """{"test":{"a":1}}"""
    )
  }

  test("SPARK-21513: to_json support map[struct, struct] to json") {
    val schema = MapType(StructType(StructField("a", IntegerType) :: Nil),
      StructType(StructField("b", IntegerType) :: Nil))
    val input = Literal(ArrayBasedMapData(Map(InternalRow(1) -> InternalRow(2))), schema)
    checkEvaluation(
      StructsToJson(Map.empty, input),
      """{"[1]":{"b":2}}"""
    )
  }

  test("SPARK-21513: to_json support map[string, integer] to json") {
    val schema = MapType(StringType, IntegerType)
    val input = Literal(ArrayBasedMapData(Map(UTF8String.fromString("a") -> 1)), schema)
    checkEvaluation(
      StructsToJson(Map.empty, input),
      """{"a":1}"""
    )
  }

  test("to_json - array with maps") {
    val inputSchema = ArrayType(MapType(StringType, IntegerType))
    val input = new GenericArrayData(
      ArrayBasedMapData(Map(UTF8String.fromString("a") -> 1)) ::
      ArrayBasedMapData(Map(UTF8String.fromString("b") -> 2)) :: Nil)
    val output = """[{"a":1},{"b":2}]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_json - array with single map") {
    val inputSchema = ArrayType(MapType(StringType, IntegerType))
    val input = new GenericArrayData(ArrayBasedMapData(Map(UTF8String.fromString("a") -> 1)) :: Nil)
    val output = """[{"a":1}]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("from/to json - interval support") {
    val schema = StructType(StructField("i", CalendarIntervalType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal.create("""{"i":"1 year 1 day"}""", StringType)),
      InternalRow(new CalendarInterval(12, 1, 0)))

    Seq(MapType(CalendarIntervalType, IntegerType), MapType(IntegerType, CalendarIntervalType))
      .foreach { dt =>
        val schema = StructField("a", dt) :: Nil
        val struct = Literal.create(null, StructType(schema))
        assert(StructsToJson(Map.empty, struct).checkInputDataTypes().isSuccess)
      }
  }

  test("from_json missing fields") {
    val input =
      """{
      |  "a": 1,
      |  "c": "foo"
      |}
      |""".stripMargin
    val jsonSchema = new StructType()
      .add("a", LongType, nullable = false)
      .add("b", StringType, nullable = false)
      .add("c", StringType, nullable = false)
    val output = InternalRow(1L, null, UTF8String.fromString("foo"))
    val expr = JsonToStructs(jsonSchema, Map.empty, Literal.create(input, StringType), UTC_OPT)
    checkEvaluation(expr, output)
    val schema = expr.dataType
    val schemaToCompare = jsonSchema.asNullable
    assert(schemaToCompare == schema)
  }

  test("SPARK-24709: infer schema of json strings") {
    checkEvaluation(new SchemaOfJson(Literal.create("""{"col":0}""")),
      "STRUCT<col: BIGINT>")
    checkEvaluation(
      new SchemaOfJson(Literal.create("""{"col0":["a"], "col1": {"col2": "b"}}""")),
      "STRUCT<col0: ARRAY<STRING>, col1: STRUCT<col2: STRING>>")
  }

  test("infer schema of JSON strings by using options") {
    checkEvaluation(
      new SchemaOfJson(Literal.create("""{"col":01}"""),
        CreateMap(Seq(Literal.create("allowNumericLeadingZeros"), Literal.create("true")))),
      "STRUCT<col: BIGINT>")
  }

  test("parse date with locale") {
    Seq("en-US", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val date = new SimpleDateFormat("yyyy-MM-dd").parse("2018-11-05")
      val schema = new StructType().add("d", DateType)
      val dateFormat = "MMM yyyy"
      val sdf = new SimpleDateFormat(dateFormat, locale)
      val dateStr = s"""{"d":"${sdf.format(date)}"}"""
      val options = Map("dateFormat" -> dateFormat, "locale" -> langTag)

      checkEvaluation(
        JsonToStructs(schema, options, Literal.create(dateStr), UTC_OPT),
        InternalRow(17836)) // number of days from 1970-01-01
    }
  }

  test("verify corrupt column") {
    checkExceptionInExpression[AnalysisException](
      JsonToStructs(
        schema = StructType.fromDDL("i int, _unparsed boolean"),
        options = Map("columnNameOfCorruptRecord" -> "_unparsed"),
        child = Literal.create("""{"i":"a"}"""),
        timeZoneId = UTC_OPT),
      expectedErrMsg = "The field for corrupt records must be string type and nullable")
  }

  def decimalInput(langTag: String): (Decimal, String) = {
    val decimalVal = new java.math.BigDecimal("1000.001")
    val decimalType = new DecimalType(10, 5)
    val expected = Decimal(decimalVal, decimalType.precision, decimalType.scale)
    val decimalFormat = new DecimalFormat("",
      new DecimalFormatSymbols(Locale.forLanguageTag(langTag)))
    val input = s"""{"d": "${decimalFormat.format(expected.toBigDecimal)}"}"""

    (expected, input)
  }

  test("parse decimals using locale") {
    def checkDecimalParsing(langTag: String): Unit = {
      val schema = new StructType().add("d", DecimalType(10, 5))
      val options = Map("locale" -> langTag)
      val (expected, input) = decimalInput(langTag)

      checkEvaluation(
        JsonToStructs(schema, options, Literal.create(input), UTC_OPT),
        InternalRow(expected))
    }

    Seq("en-US", "ko-KR", "ru-RU", "de-DE").foreach(checkDecimalParsing)
  }

  test("inferring the decimal type using locale") {
    def checkDecimalInfer(langTag: String, expectedType: String): Unit = {
      val options = Map("locale" -> langTag, "prefersDecimal" -> "true")
      val (_, input) = decimalInput(langTag)

      checkEvaluation(
        SchemaOfJson(Literal.create(input), options),
        expectedType)
    }

    Seq("en-US", "ko-KR", "ru-RU", "de-DE").foreach {
        checkDecimalInfer(_, """STRUCT<d: DECIMAL(7,3)>""")
    }
  }

  test("Length of JSON array") {
    Seq(
      ("", null),
      ("[1,2,3]", 3),
      ("[]", 0),
      ("[[1],[2,3],[]]", 3),
      ("""[{"a":123},{"b":"hello"}]""", 2),
      ("""[1,2,3,[33,44],{"key":[2,3,4]}]""", 5),
      ("""[1,2,3,4,5""", null),
      ("Random String", null),
      ("""{"key":"not a json array"}""", null),
      ("""{"key": 25}""", null)
    ).foreach {
      case(literal, expectedValue) =>
        checkEvaluation(LengthOfJsonArray(Literal(literal)), expectedValue)
    }
  }

  test("json_object_keys") {
    Seq(
      // Invalid inputs
      ("", null),
      ("[]", null),
      ("""[{"key": "JSON"}]""", null),
      ("""{"key": 45, "random_string"}""", null),
      ("""{[1, 2, {"Key": "Invalid JSON"}]}""", null),
      // JSON objects
      ("{}", Seq.empty[UTF8String]),
      ("""{"key": 1}""", Seq("key")),
      ("""{"key": "value", "key2": 2}""", Seq("key", "key2")),
      ("""{"arrayKey": [1, 2, 3]}""", Seq("arrayKey")),
      ("""{"key":[1,2,3,{"key":"value"},[1,2,3]]}""", Seq("key")),
      ("""{"f1":"abc","f2":{"f3":"a", "f4":"b"}}""", Seq("f1", "f2")),
      ("""{"k1": [1, 2, {"key": 5}], "k2": {"key2": [1, 2]}}""", Seq("k1", "k2"))
    ).foreach {
      case (input, expected) =>
        checkEvaluation(JsonObjectKeys(Literal(input)), expected)
    }
  }

  test("SPARK-35320: from_json should fail with a key type different of StringType") {
    Seq(
      (MapType(IntegerType, StringType), """{"1": "test"}"""),
      (StructType(Seq(StructField("test", MapType(IntegerType, StringType)))),
        """"test": {"1": "test"}"""),
      (ArrayType(MapType(IntegerType, StringType)), """[{"1": "test"}]"""),
      (MapType(StringType, MapType(IntegerType, StringType)), """{"key": {"1" : "test"}}""")
    ).foreach{
      case(schema, jsonData) =>
        assert(JsonToStructs(schema, Map.empty, Literal(jsonData)).checkInputDataTypes().isFailure)
      }
  }
}
