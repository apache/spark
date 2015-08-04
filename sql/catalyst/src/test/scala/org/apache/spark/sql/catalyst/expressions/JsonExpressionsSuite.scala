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
}
