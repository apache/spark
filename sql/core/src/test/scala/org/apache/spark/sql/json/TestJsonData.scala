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

package org.apache.spark.sql.json

import org.apache.spark.sql.test.TestSQLContext

object TestJsonData {

  val primitiveFieldAndType =
    TestSQLContext.sparkContext.parallelize(
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "bigInteger":92233720368547758070,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }"""  :: Nil)

  val complexFieldAndType =
    TestSQLContext.sparkContext.parallelize(
      """{"struct":{"field1": true, "field2": 92233720368547758070},
          "structWithArrayFields":{"field1":[4, 5, 6], "field2":["str1", "str2"]},
          "arrayOfString":["str1", "str2"],
          "arrayOfInteger":[1, 2147483647, -2147483648],
          "arrayOfLong":[21474836470, 9223372036854775807, -9223372036854775808],
          "arrayOfBigInteger":[922337203685477580700, -922337203685477580800],
          "arrayOfDouble":[1.2, 1.7976931348623157E308, 4.9E-324, 2.2250738585072014E-308],
          "arrayOfBoolean":[true, false, true],
          "arrayOfNull":[null, null, null, null],
          "arrayOfStruct":[{"field1": true, "field2": "str1"}, {"field1": false}, {"field3": null}],
          "arrayOfArray1":[[1, 2, 3], ["str1", "str2"]],
          "arrayOfArray2":[[1, 2, 3], [1.1, 2.1, 3.1]]
         }"""  :: Nil)

  val primitiveFieldValueTypeConflict =
    TestSQLContext.sparkContext.parallelize(
      """{"num_num_1":11, "num_num_2":null, "num_num_3": 1.1,
          "num_bool":true, "num_str":13.1, "str_bool":"str1"}""" ::
      """{"num_num_1":null, "num_num_2":21474836470.9, "num_num_3": null,
          "num_bool":12, "num_str":null, "str_bool":true}""" ::
      """{"num_num_1":21474836470, "num_num_2":92233720368547758070, "num_num_3": 100,
          "num_bool":false, "num_str":"str1", "str_bool":false}""" ::
      """{"num_num_1":21474836570, "num_num_2":1.1, "num_num_3": 21474836470,
          "num_bool":null, "num_str":92233720368547758070, "str_bool":null}""" :: Nil)

  val complexFieldValueTypeConflict =
    TestSQLContext.sparkContext.parallelize(
      """{"num_struct":11, "str_array":[1, 2, 3],
          "array":[], "struct_array":[], "struct": {}}""" ::
      """{"num_struct":{"field":false}, "str_array":null,
          "array":null, "struct_array":{}, "struct": null}""" ::
      """{"num_struct":null, "str_array":"str",
          "array":[4, 5, 6], "struct_array":[7, 8, 9], "struct": {"field":null}}""" ::
      """{"num_struct":{}, "str_array":["str1", "str2", 33],
          "array":[7], "struct_array":{"field": true}, "struct": {"field": "str"}}""" :: Nil)

  val arrayElementTypeConflict =
    TestSQLContext.sparkContext.parallelize(
      """{"array1": [1, 1.1, true, null, [], {}, [2,3,4], {"field":"str"}],
          "array2": [{"field":214748364700}, {"field":1}]}""" :: Nil)

  val missingFields =
    TestSQLContext.sparkContext.parallelize(
      """{"a":true}""" ::
      """{"b":21474836470}""" ::
      """{"c":[33, 44]}""" ::
      """{"d":{"field":true}}""" ::
      """{"e":"str"}""" :: Nil)

  val complexFieldAndType2 =
    TestSQLContext.sparkContext.parallelize(
      """{"arrayOfStruct":[{"field1": true, "field2": "str1"}, {"field1": false}, {"field3": null}],
          "complexArrayOfStruct": [
          {
            "field1": [
            {
              "inner1": "str1"
            },
            {
              "inner2": ["str2", "str22"]
            }],
            "field2": [[1, 2], [3, 4]]
          },
          {
            "field1": [
            {
              "inner2": ["str3", "str33"]
            },
            {
              "inner1": "str4"
            }],
            "field2": [[5, 6], [7, 8]]
          }],
          "arrayOfArray1": [
          [
            [5]
          ],
          [
            [6, 7],
            [8]
          ]],
          "arrayOfArray2": [
          [
            [
              {
                "inner1": "str1"
              }
            ]
          ],
          [
            [],
            [
              {"inner2": ["str3", "str33"]},
              {"inner2": ["str4"], "inner1": "str11"}
            ]
          ],
          [
            [
              {"inner3": [[{"inner4": 2}]]}
            ]
          ]]
      }""" :: Nil)

  val jsonArray =
    TestSQLContext.sparkContext.parallelize(
      """[{"a":"str_a_1"}]""" ::
      """[{"a":"str_a_2"}, {"b":"str_b_3"}]""" ::
      """{"b":"str_b_4", "a":"str_a_4", "c":"str_c_4"}""" ::
      """[]""" :: Nil)
}
