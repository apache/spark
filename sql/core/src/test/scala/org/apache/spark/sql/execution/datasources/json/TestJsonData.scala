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

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

private[json] trait TestJsonData {
  protected def sqlContext: SQLContext

  def primitiveFieldAndType: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "bigInteger":92233720368547758070,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }"""  :: Nil)

  def primitiveFieldValueTypeConflict: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"num_num_1":11, "num_num_2":null, "num_num_3": 1.1,
          "num_bool":true, "num_str":13.1, "str_bool":"str1"}""" ::
      """{"num_num_1":null, "num_num_2":21474836470.9, "num_num_3": null,
          "num_bool":12, "num_str":null, "str_bool":true}""" ::
      """{"num_num_1":21474836470, "num_num_2":92233720368547758070, "num_num_3": 100,
          "num_bool":false, "num_str":"str1", "str_bool":false}""" ::
      """{"num_num_1":21474836570, "num_num_2":1.1, "num_num_3": 21474836470,
          "num_bool":null, "num_str":92233720368547758070, "str_bool":null}""" :: Nil)

  def jsonNullStruct: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"nullstr":"","ip":"27.31.100.29","headers":{"Host":"1.abc.com","Charset":"UTF-8"}}""" ::
        """{"nullstr":"","ip":"27.31.100.29","headers":{}}""" ::
        """{"nullstr":"","ip":"27.31.100.29","headers":""}""" ::
        """{"nullstr":null,"ip":"27.31.100.29","headers":null}""" :: Nil)

  def complexFieldValueTypeConflict: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"num_struct":11, "str_array":[1, 2, 3],
          "array":[], "struct_array":[], "struct": {}}""" ::
      """{"num_struct":{"field":false}, "str_array":null,
          "array":null, "struct_array":{}, "struct": null}""" ::
      """{"num_struct":null, "str_array":"str",
          "array":[4, 5, 6], "struct_array":[7, 8, 9], "struct": {"field":null}}""" ::
      """{"num_struct":{}, "str_array":["str1", "str2", 33],
          "array":[7], "struct_array":{"field": true}, "struct": {"field": "str"}}""" :: Nil)

  def arrayElementTypeConflict: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"array1": [1, 1.1, true, null, [], {}, [2,3,4], {"field":"str"}],
          "array2": [{"field":214748364700}, {"field":1}]}""" ::
      """{"array3": [{"field":"str"}, {"field":1}]}""" ::
      """{"array3": [1, 2, 3]}""" :: Nil)

  def missingFields: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"a":true}""" ::
      """{"b":21474836470}""" ::
      """{"c":[33, 44]}""" ::
      """{"d":{"field":true}}""" ::
      """{"e":"str"}""" :: Nil)

  def complexFieldAndType1: RDD[String] =
    sqlContext.sparkContext.parallelize(
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

  def complexFieldAndType2: RDD[String] =
    sqlContext.sparkContext.parallelize(
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

  def mapType1: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"map": {"a": 1}}""" ::
      """{"map": {"b": 2}}""" ::
      """{"map": {"c": 3}}""" ::
      """{"map": {"c": 1, "d": 4}}""" ::
      """{"map": {"e": null}}""" :: Nil)

  def mapType2: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"map": {"a": {"field1": [1, 2, 3, null]}}}""" ::
      """{"map": {"b": {"field2": 2}}}""" ::
      """{"map": {"c": {"field1": [], "field2": 4}}}""" ::
      """{"map": {"c": {"field2": 3}, "d": {"field1": [null]}}}""" ::
      """{"map": {"e": null}}""" ::
      """{"map": {"f": {"field1": null}}}""" :: Nil)

  def nullsInArrays: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"field1":[[null], [[["Test"]]]]}""" ::
      """{"field2":[null, [{"Test":1}]]}""" ::
      """{"field3":[[null], [{"Test":"2"}]]}""" ::
      """{"field4":[[null, [1,2,3]]]}""" :: Nil)

  def jsonArray: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """[{"a":"str_a_1"}]""" ::
      """[{"a":"str_a_2"}, {"b":"str_b_3"}]""" ::
      """{"b":"str_b_4", "a":"str_a_4", "c":"str_c_4"}""" ::
      """[]""" :: Nil)

  def corruptRecords: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{""" ::
      """""" ::
      """{"a":1, b:2}""" ::
      """{"a":{, b:3}""" ::
      """{"b":"str_b_4", "a":"str_a_4", "c":"str_c_4"}""" ::
      """]""" :: Nil)

  def additionalCorruptRecords: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{"dummy":"test"}""" ::
      """[1,2,3]""" ::
      """":"test", "a":1}""" ::
      """42""" ::
      """     ","ian":"test"}""" :: Nil)

  def emptyRecords: RDD[String] =
    sqlContext.sparkContext.parallelize(
      """{""" ::
        """""" ::
        """{"a": {}}""" ::
        """{"a": {"b": {}}}""" ::
        """{"b": [{"c": {}}]}""" ::
        """]""" :: Nil)

  lazy val singleRow: RDD[String] = sqlContext.sparkContext.parallelize("""{"a":123}""" :: Nil)

  def empty: RDD[String] = sqlContext.sparkContext.parallelize(Seq[String]())
}
