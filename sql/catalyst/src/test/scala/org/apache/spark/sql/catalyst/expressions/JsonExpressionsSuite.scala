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

import java.util.Calendar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils, GenericArrayData, PermissiveMode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class JsonExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
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

  val jsonTupleQuery = Literal("f1") ::
    Literal("f2") ::
    Literal("f3") ::
    Literal("f4") ::
    Literal("f5") ::
    Nil

  private def checkJsonTuple(jt: JsonTuple, expected: InternalRow): Unit = {
    assert(jt.eval(null).toSeq.head === expected)
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

  val gmtId = Option(DateTimeUtils.TimeZoneGMT.getID)

  test("from_json") {
    val jsonData = """{"a": 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData), gmtId),
      InternalRow(1)
    )
  }

  test("from_json - invalid data") {
    val jsonData = """{"a" 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData), gmtId),
      null
    )

    // Other modes should still return `null`.
    checkEvaluation(
      JsonToStructs(schema, Map("mode" -> PermissiveMode.name), Literal(jsonData), gmtId),
      null
    )
  }

  test("from_json - input=array, schema=array, output=array") {
    val input = """[{"a": 1}, {"a": 2}]"""
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = InternalRow(1) :: InternalRow(2) :: Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=object, schema=array, output=array of single row") {
    val input = """{"a": 1}"""
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = InternalRow(1) :: Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=empty array, schema=array, output=empty array") {
    val input = "[ ]"
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=empty object, schema=array, output=array of single row with null") {
    val input = "{ }"
    val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val output = InternalRow(null) :: Nil
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=array of single object, schema=struct, output=single row") {
    val input = """[{"a": 1}]"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(1)
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=array, schema=struct, output=null") {
    val input = """[{"a": 1}, {"a": 2}]"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = null
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=empty array, schema=struct, output=null") {
    val input = """[]"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = null
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json - input=empty object, schema=struct, output=single row with null") {
    val input = """{  }"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(null)
    checkEvaluation(JsonToStructs(schema, Map.empty, Literal(input), gmtId), output)
  }

  test("from_json null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal.create(null, StringType), gmtId),
      null
    )
  }

  test("SPARK-20549: from_json bad UTF-8") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(badJson), gmtId),
      null)
  }

  test("from_json with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)

    val jsonData1 = """{"t": "2016-01-01T00:00:00.123Z"}"""
    var c = Calendar.getInstance(DateTimeUtils.TimeZoneGMT)
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 123)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData1), gmtId),
      InternalRow(c.getTimeInMillis * 1000L)
    )
    // The result doesn't change because the json string includes timezone string ("Z" here),
    // which means the string represents the timestamp string in the timezone regardless of
    // the timeZoneId parameter.
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData1), Option("PST")),
      InternalRow(c.getTimeInMillis * 1000L)
    )

    val jsonData2 = """{"t": "2016-01-01T00:00:00"}"""
    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      c = Calendar.getInstance(tz)
      c.set(2016, 0, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkEvaluation(
        JsonToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"),
          Literal(jsonData2),
          Option(tz.getID)),
        InternalRow(c.getTimeInMillis * 1000L)
      )
      checkEvaluation(
        JsonToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
            DateTimeUtils.TIMEZONE_OPTION -> tz.getID),
          Literal(jsonData2),
          gmtId),
        InternalRow(c.getTimeInMillis * 1000L)
      )
    }
  }

  test("SPARK-19543: from_json empty input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal.create(" ", StringType), gmtId),
      null
    )
  }

  test("to_json - struct") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    checkEvaluation(
      StructsToJson(Map.empty, struct, gmtId),
      """{"a":1}"""
    )
  }

  test("to_json - array") {
    val inputSchema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val input = new GenericArrayData(InternalRow(1) :: InternalRow(2) :: Nil)
    val output = """[{"a":1},{"a":2}]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), gmtId),
      output)
  }

  test("to_json - array with single empty row") {
    val inputSchema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val input = new GenericArrayData(InternalRow(null) :: Nil)
    val output = """[{}]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), gmtId),
      output)
  }

  test("to_json - empty array") {
    val inputSchema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val input = new GenericArrayData(Nil)
    val output = """[]"""
    checkEvaluation(
      StructsToJson(Map.empty, Literal.create(input, inputSchema), gmtId),
      output)
  }

  test("to_json null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(null, schema)
    checkEvaluation(
      StructsToJson(Map.empty, struct, gmtId),
      null
    )
  }

  test("to_json with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)
    val c = Calendar.getInstance(DateTimeUtils.TimeZoneGMT)
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    val struct = Literal.create(create_row(c.getTimeInMillis * 1000L), schema)

    checkEvaluation(
      StructsToJson(Map.empty, struct, gmtId),
      """{"t":"2016-01-01T00:00:00.000Z"}"""
    )
    checkEvaluation(
      StructsToJson(Map.empty, struct, Option("PST")),
      """{"t":"2015-12-31T16:00:00.000-08:00"}"""
    )

    checkEvaluation(
      StructsToJson(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> gmtId.get),
        struct,
        gmtId),
      """{"t":"2016-01-01T00:00:00"}"""
    )
    checkEvaluation(
      StructsToJson(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> "PST"),
        struct,
        gmtId),
      """{"t":"2015-12-31T16:00:00"}"""
    )
  }

  test("to_json: verify MapType's value type instead of key type") {
    // Keys in map are treated as strings when converting to JSON. The type doesn't matter at all.
    val mapType1 = MapType(CalendarIntervalType, IntegerType)
    val schema1 = StructType(StructField("a", mapType1) :: Nil)
    val struct1 = Literal.create(null, schema1)
    checkEvaluation(
      StructsToJson(Map.empty, struct1, gmtId),
      null
    )

    // The value type must be valid for converting to JSON.
    val mapType2 = MapType(IntegerType, CalendarIntervalType)
    val schema2 = StructType(StructField("a", mapType2) :: Nil)
    val struct2 = Literal.create(null, schema2)
    intercept[TreeNodeException[_]] {
      checkEvaluation(
        StructsToJson(Map.empty, struct2, gmtId),
        null
      )
    }
  }
}
