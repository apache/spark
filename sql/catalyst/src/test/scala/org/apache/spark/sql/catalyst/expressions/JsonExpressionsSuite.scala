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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ParseModes
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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
      InternalRow.fromSeq(Seq(null, null, null, null, null)))
  }

  test("json_tuple - hive key 5 - null and empty fields") {
    checkJsonTuple(
      JsonTuple(Literal("""{"f1": "", "f5": null}""") :: jsonTupleQuery),
      InternalRow.fromSeq(Seq(UTF8String.fromString(""), null, null, null, null)))
  }

  test("json_tuple - hive key 6 - invalid json (array)") {
    checkJsonTuple(
      JsonTuple(Literal("[invalid JSON string]") :: jsonTupleQuery),
      InternalRow.fromSeq(Seq(null, null, null, null, null)))
  }

  test("json_tuple - invalid json (object start only)") {
    checkJsonTuple(
      JsonTuple(Literal("{") :: jsonTupleQuery),
      InternalRow.fromSeq(Seq(null, null, null, null, null)))
  }

  test("json_tuple - invalid json (no object end)") {
    checkJsonTuple(
      JsonTuple(Literal("""{"foo": "bar"""") :: jsonTupleQuery),
      InternalRow.fromSeq(Seq(null, null, null, null, null)))
  }

  test("json_tuple - invalid json (invalid json)") {
    checkJsonTuple(
      JsonTuple(Literal("\\") :: jsonTupleQuery),
      InternalRow.fromSeq(Seq(null, null, null, null, null)))
  }

  test("json_tuple - preserve newlines") {
    checkJsonTuple(
      JsonTuple(Literal("{\"a\":\"b\nc\"}") :: Literal("a") :: Nil),
      InternalRow.fromSeq(Seq(UTF8String.fromString("b\nc"))))
  }

  test("from_json") {
    val jsonData = """{"a": 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStruct(schema, Map.empty, Literal(jsonData)),
      InternalRow.fromSeq(1 :: Nil)
    )
  }

  test("from_json - invalid data") {
    val jsonData = """{"a" 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStruct(schema, Map.empty, Literal(jsonData)),
      null
    )

    // Other modes should still return `null`.
    checkEvaluation(
      JsonToStruct(schema, Map("mode" -> ParseModes.PERMISSIVE_MODE), Literal(jsonData)),
      null
    )
  }

  test("from_json null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStruct(schema, Map.empty, Literal.create(null, StringType)),
      null
    )
  }

  test("to_json") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    checkEvaluation(
      StructToJson(Map.empty, struct),
      """{"a":1}"""
    )
  }

  test("to_json null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(null, schema)
    checkEvaluation(
      StructToJson(Map.empty, struct),
      null
    )
  }
}
