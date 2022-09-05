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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

class SelectedFieldSuite extends AnalysisTest {
  private val ignoredField = StructField("col1", StringType, nullable = false)

  // The test schema as a tree string, i.e. `schema.treeString`
  // root
  //  |-- col1: string (nullable = false)
  //  |-- col2: struct (nullable = true)
  //  |    |-- field1: integer (nullable = true)
  //  |    |-- field6: struct (nullable = true)
  //  |    |    |-- subfield1: string (nullable = false)
  //  |    |    |-- subfield2: string (nullable = true)
  //  |    |-- field7: struct (nullable = true)
  //  |    |    |-- subfield1: struct (nullable = true)
  //  |    |    |    |-- subsubfield1: integer (nullable = true)
  //  |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |-- field9: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: integer (valueContainsNull = false)
  private val nestedComplex = StructType(ignoredField ::
      StructField("col2", StructType(
        StructField("field1", IntegerType) ::
        StructField("field6", StructType(
          StructField("subfield1", StringType, nullable = false) ::
          StructField("subfield2", StringType) :: Nil)) ::
        StructField("field7", StructType(
          StructField("subfield1", StructType(
            StructField("subsubfield1", IntegerType) ::
            StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)) ::
        StructField("field9",
          MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil)) :: Nil)

  test("SelectedField should not match an attribute reference") {
    val testRelation = LocalRelation(nestedComplex.toAttributes)
    assertResult(None)(unapplySelect("col1", testRelation))
    assertResult(None)(unapplySelect("col1 as foo", testRelation))
    assertResult(None)(unapplySelect("col2", testRelation))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col2: struct (nullable = true)
  //  |    |-- field2: array (nullable = true)
  //  |    |    |-- element: integer (containsNull = false)
  //  |    |-- field3: array (nullable = false)
  //  |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  //  |    |    |    |-- subfield3: array (nullable = true)
  //  |    |    |    |    |-- element: integer (containsNull = true)
  private val structOfArray = StructType(ignoredField ::
    StructField("col2", StructType(
      StructField("field2", ArrayType(IntegerType, containsNull = false)) ::
      StructField("field3", ArrayType(StructType(
        StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) ::
          StructField("subfield3", ArrayType(IntegerType)) :: Nil)), nullable = false)
      :: Nil))
    :: Nil)

  testSelect(structOfArray, "col2.field2", "col2.field2[0] as foo") {
    StructField("col2", StructType(
      StructField("field2", ArrayType(IntegerType, containsNull = false)) :: Nil))
  }

  testSelect(nestedComplex, "col2.field9", "col2.field9['foo'] as foo") {
    StructField("col2", StructType(
      StructField("field9", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil))
  }

  testSelect(structOfArray, "col2.field3.subfield3", "col2.field3[0].subfield3 as foo",
      "col2.field3.subfield3[0] as foo", "col2.field3[0].subfield3[0] as foo") {
    StructField("col2", StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield3", ArrayType(IntegerType)) :: Nil)), nullable = false) :: Nil))
  }

  testSelect(structOfArray, "element_at(col2.field3, 1).subfield3 as foo",
    "element_at(col2.field3.subfield3, 0) as foo",
    "element_at(element_at(col2.field3, 1).subfield3, 0) as foo") {
    StructField("col2", StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield3", ArrayType(IntegerType)) :: Nil)), nullable = false) :: Nil))
  }

  testSelect(structOfArray, "col2.field3.subfield1") {
    StructField("col2", StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield1", IntegerType) :: Nil)), nullable = false) :: Nil))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col2: struct (nullable = true)
  //  |    |-- field4: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: struct (valueContainsNull = false)
  //  |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |-- subfield2: array (nullable = true)
  //  |    |    |    |    |-- element: integer (containsNull = false)
  //  |    |-- field8: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: array (valueContainsNull = false)
  //  |    |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: array (nullable = true)
  //  |    |    |    |    |    |-- element: integer (containsNull = false)
  private val structWithMap = StructType(
    ignoredField ::
    StructField("col2", StructType(
      StructField("field4", MapType(StringType, StructType(
        StructField("subfield1", IntegerType) ::
          StructField("subfield2", ArrayType(IntegerType, containsNull = false)) :: Nil
      ), valueContainsNull = false)) ::
      StructField("field8", MapType(StringType, ArrayType(StructType(
        StructField("subfield1", IntegerType) ::
          StructField("subfield2", ArrayType(IntegerType, containsNull = false)) :: Nil)
      ), valueContainsNull = false)) :: Nil
    )) :: Nil
  )

  testSelect(structWithMap, "col2.field4['foo'].subfield1 as foo") {
    StructField("col2", StructType(
      StructField("field4", MapType(StringType, StructType(
        StructField("subfield1", IntegerType) :: Nil), valueContainsNull = false)) :: Nil))
  }

  testSelect(structWithMap,
    "col2.field4['foo'].subfield2 as foo", "col2.field4['foo'].subfield2[0] as foo") {
    StructField("col2", StructType(
      StructField("field4", MapType(StringType, StructType(
        StructField("subfield2", ArrayType(IntegerType, containsNull = false))
          :: Nil), valueContainsNull = false)) :: Nil))
  }

  testSelect(structWithMap, "element_at(col2.field4, 'foo').subfield2 as foo",
    "element_at(element_at(col2.field4, 'foo').subfield2, 1) as foo") {
    StructField("col2", StructType(
      StructField("field4", MapType(StringType, StructType(
        StructField("subfield2", ArrayType(IntegerType, containsNull = false))
          :: Nil), valueContainsNull = false)) :: Nil))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col2: struct (nullable = true)
  //  |    |-- field5: array (nullable = false)
  //  |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |-- subfield1: struct (nullable = false)
  //  |    |    |    |    |-- subsubfield1: integer (nullable = true)
  //  |    |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |    |    |-- subfield2: struct (nullable = true)
  //  |    |    |    |    |-- subsubfield1: struct (nullable = true)
  //  |    |    |    |    |    |-- subsubsubfield1: string (nullable = true)
  //  |    |    |    |    |-- subsubfield2: integer (nullable = true)
  private val structWithArray = StructType(
    ignoredField ::
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) ::
          StructField("subsubfield2", IntegerType) :: Nil), nullable = false) ::
        StructField("subfield2", StructType(
          StructField("subsubfield1", StructType(
            StructField("subsubsubfield1", StringType) :: Nil)) ::
          StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)), nullable = false) :: Nil)
    ) :: Nil
  )

  testSelect(structWithArray, "col2.field5.subfield1") {
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) ::
          StructField("subsubfield2", IntegerType) :: Nil), nullable = false)
          :: Nil)), nullable = false) :: Nil))
  }

  testSelect(structWithArray, "col2.field5.subfield1.subsubfield1") {
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) :: Nil), nullable = false)
          :: Nil)), nullable = false) :: Nil))
  }

  testSelect(structWithArray, "col2.field5.subfield2.subsubfield1.subsubsubfield1") {
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield2", StructType(
          StructField("subsubfield1", StructType(
            StructField("subsubsubfield1", StringType) :: Nil)) :: Nil))
          :: Nil)), nullable = false) :: Nil))
  }

  testSelect(structWithMap, "col2.field8['foo'][0].subfield1 as foo") {
    StructField("col2", StructType(
      StructField("field8", MapType(StringType, ArrayType(StructType(
        StructField("subfield1", IntegerType) :: Nil)), valueContainsNull = false)) :: Nil))
  }

  testSelect(nestedComplex, "col2.field1") {
    StructField("col2", StructType(
      StructField("field1", IntegerType) :: Nil))
  }

  testSelect(nestedComplex, "col2.field6") {
    StructField("col2", StructType(
      StructField("field6", StructType(
        StructField("subfield1", StringType, nullable = false) ::
        StructField("subfield2", StringType) :: Nil)) :: Nil))
  }

  testSelect(nestedComplex, "col2.field6.subfield1") {
    StructField("col2", StructType(
      StructField("field6", StructType(
        StructField("subfield1", StringType, nullable = false) :: Nil)) :: Nil))
  }

  testSelect(nestedComplex, "col2.field7.subfield1") {
    StructField("col2", StructType(
      StructField("field7", StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) ::
          StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)) :: Nil))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col3: array (nullable = false)
  //  |    |-- element: struct (containsNull = false)
  //  |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = false)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  //  |    |    |-- field2: map (nullable = true)
  //  |    |    |    |-- key: string
  //  |    |    |    |-- value: integer (valueContainsNull = false)
  private val arrayWithStructAndMap = StructType(Array(
    StructField("col3", ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType, nullable = false) ::
        StructField("subfield2", IntegerType) :: Nil)) ::
      StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false))
      :: Nil), containsNull = false), nullable = false)
  ))

  testSelect(arrayWithStructAndMap, "col3.field1.subfield1") {
    StructField("col3", ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil))
        :: Nil), containsNull = true), nullable = false)
  }

  testSelect(arrayWithStructAndMap, "col3.field2['foo'] as foo") {
    StructField("col3", ArrayType(StructType(
      StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false))
        :: Nil), containsNull = true), nullable = false)
  }

  testSelect(arrayWithStructAndMap, "element_at(col3.field2, 'foo') as foo") {
    StructField("col3", ArrayType(StructType(
      StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false))
        :: Nil), containsNull = true), nullable = false)
  }

  //  |-- col1: string (nullable = false)
  //  |-- col4: map (nullable = false)
  //  |    |-- key: string
  //  |    |-- value: struct (valueContainsNull = false)
  //  |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = false)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  //  |    |    |-- field2: map (nullable = true)
  //  |    |    |    |-- key: string
  //  |    |    |    |-- value: integer (valueContainsNull = false)
  private val col4 = StructType(Array(ignoredField,
    StructField("col4", MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType, nullable = false) ::
        StructField("subfield2", IntegerType) :: Nil)) ::
        StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false))
        :: Nil), valueContainsNull = false), nullable = false)
  ))

  testSelect(col4, "col4['foo'].field1.subfield1 as foo") {
    StructField("col4", MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil))
        :: Nil), valueContainsNull = false), nullable = false)
  }

  testSelect(col4, "col4['foo'].field2['bar'] as foo") {
    StructField("col4", MapType(StringType, StructType(
      StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false))
        :: Nil), valueContainsNull = false), nullable = false)
  }

  //  |-- col1: string (nullable = false)
  //  |-- col5: array (nullable = true)
  //  |    |-- element: map (containsNull = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: struct (valueContainsNull = false)
  //  |    |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: integer (nullable = true)
  private val arrayOfStruct = StructType(Array(ignoredField,
    StructField("col5", ArrayType(MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) ::
        StructField("subfield2", IntegerType) :: Nil)) :: Nil), valueContainsNull = false)))
  ))

  testSelect(arrayOfStruct, "col5[0]['foo'].field1.subfield1 as foo") {
    StructField("col5", ArrayType(MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) :: Nil)) :: Nil), valueContainsNull = false)))
  }

  testSelect(arrayOfStruct, "map_values(col5[0]).field1.subfield1 as foo") {
    StructField("col5", ArrayType(MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) :: Nil)) :: Nil), valueContainsNull = false)))
  }

  testSelect(arrayOfStruct, "map_values(col5[0]).field1.subfield2 as foo") {
    StructField("col5", ArrayType(MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield2", IntegerType) :: Nil)) :: Nil), valueContainsNull = false)))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col6: map (nullable = true)
  //  |    |-- key: string
  //  |    |-- value: array (valueContainsNull = true)
  //  |    |    |-- element: struct (containsNull = false)
  //  |    |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: integer (nullable = true)
  private val mapOfArray = StructType(Array(ignoredField,
    StructField("col6", MapType(StringType, ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) ::
        StructField("subfield2", IntegerType) :: Nil)) :: Nil), containsNull = false)))))

  testSelect(mapOfArray, "col6['foo'][0].field1.subfield1 as foo") {
    StructField("col6", MapType(StringType, ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  testSelect(mapOfArray, "element_at(element_at(col6, 'foo'), 0).field1.subfield1 as foo") {
    StructField("col6", MapType(StringType, ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  // An array with a struct with a different fields
  //  |-- col1: string (nullable = false)
  //  |-- col7: array (nullable = true)
  //  |    |-- element: struct (containsNull = true)
  //  |    |    |-- field1: integer (nullable = false)
  //  |    |    |-- field2: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = false)
  //  |    |    |-- field3: array (nullable = true)
  //  |    |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = false)
  private val arrayWithMultipleFields = StructType(Array(ignoredField,
    StructField("col7", ArrayType(StructType(
    StructField("field1", IntegerType, nullable = false) ::
      StructField("field2", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil)) ::
      StructField("field3", ArrayType(StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil))) :: Nil)))))

  testSelect(arrayWithMultipleFields,
    "col7.field1", "col7[0].field1 as foo", "col7.field1[0] as foo") {
    StructField("col7", ArrayType(StructType(
      StructField("field1", IntegerType, nullable = false) :: Nil)))
  }

  testSelect(arrayWithMultipleFields,
    "col7.field1", "element_at(col7, 0).field1 as foo", "element_at(col7.field1, 0) as foo") {
    StructField("col7", ArrayType(StructType(
      StructField("field1", IntegerType, nullable = false) :: Nil)))
  }

  testSelect(arrayWithMultipleFields, "col7.field2.subfield1") {
    StructField("col7", ArrayType(StructType(
      StructField("field2", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil)) :: Nil)))
  }

  testSelect(arrayWithMultipleFields, "col7.field3.subfield1") {
    StructField("col7", ArrayType(StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil))) :: Nil)))
  }

  // Array with a nested int array
  //  |-- col1: string (nullable = false)
  //  |-- col8: array (nullable = true)
  //  |    |-- element: struct (containsNull = true)
  //  |    |    |-- field1: array (nullable = false)
  //  |    |    |    |-- element: integer (containsNull = false)
  private val arrayOfArray = StructType(Array(ignoredField,
    StructField("col8",
      ArrayType(StructType(Array(StructField("field1",
        ArrayType(IntegerType, containsNull = false), nullable = false))))
    )))

  testSelect(arrayOfArray, "col8.field1",
    "col8[0].field1 as foo",
    "col8.field1[0] as foo",
    "col8[0].field1[0] as foo") {
    StructField("col8", ArrayType(StructType(
      StructField("field1", ArrayType(IntegerType, containsNull = false), nullable = false)
        :: Nil)))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col2: map (nullable = true)
  //  |    |-- key: struct (containsNull = false)
  //  |    |     |-- field1: string (nullable = true)
  //  |    |     |-- field2: integer (nullable = true)
  //  |    |-- value: array (valueContainsNull = true)
  //  |    |    |-- element: struct (containsNull = false)
  //  |    |    |    |-- field3: struct (nullable = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: integer (nullable = true)
  private val mapWithStructKey = StructType(Array(ignoredField,
    StructField("col2", MapType(
      StructType(
        StructField("field1", StringType) ::
        StructField("field2", IntegerType) :: Nil),
      ArrayType(StructType(
        StructField("field3", StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) :: Nil)) :: Nil), containsNull = false)))))

  testSelect(mapWithStructKey, "map_keys(col2).field1 as foo") {
    StructField("col2", MapType(
      StructType(StructField("field1", StringType) :: Nil),
      ArrayType(StructType(
        StructField("field3", StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  testSelect(mapWithStructKey, "map_keys(col2).field2 as foo") {
    StructField("col2", MapType(
      StructType(StructField("field2", IntegerType) :: Nil),
      ArrayType(StructType(
        StructField("field3", StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  //  |-- col1: string (nullable = false)
  //  |-- col2: map (nullable = true)
  //  |    |-- key: array (valueContainsNull = true)
  //  |    |     |-- element: struct (containsNull = false)
  //  |    |     |     |-- field1: string (nullable = true)
  //  |    |     |     |-- field2: struct (containsNull = false)
  //  |    |     |     |     |-- subfield1: integer (nullable = true)
  //  |    |     |     |     |-- subfield2: long (nullable = true)
  //  |    |-- value: array (valueContainsNull = true)
  //  |    |    |-- element: struct (containsNull = false)
  //  |    |    |    |-- field3: struct (nullable = true)
  //  |    |    |    |    |-- subfield3: integer (nullable = true)
  //  |    |    |    |    |-- subfield4: integer (nullable = true)
  private val mapWithArrayOfStructKey = StructType(Array(ignoredField,
    StructField("col2", MapType(
      ArrayType(StructType(
        StructField("field1", StringType) ::
        StructField("field2", StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", LongType) :: Nil)) :: Nil), containsNull = false),
      ArrayType(StructType(
        StructField("field3", StructType(
          StructField("subfield3", IntegerType) ::
          StructField("subfield4", IntegerType) :: Nil)) :: Nil), containsNull = false)))))

  testSelect(mapWithArrayOfStructKey, "map_keys(col2)[0].field1 as foo") {
    StructField("col2", MapType(
      ArrayType(StructType(
        StructField("field1", StringType) :: Nil), containsNull = true),
      ArrayType(StructType(
        StructField("field3", StructType(
          StructField("subfield3", IntegerType) ::
          StructField("subfield4", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  testSelect(mapWithArrayOfStructKey, "map_keys(col2)[0].field2.subfield1 as foo") {
    StructField("col2", MapType(
      ArrayType(StructType(
        StructField("field2", StructType(
          StructField("subfield1", IntegerType) :: Nil)) :: Nil), containsNull = true),
      ArrayType(StructType(
        StructField("field3", StructType(
          StructField("subfield3", IntegerType) ::
          StructField("subfield4", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  def assertResult(expected: StructField)(actual: StructField)(selectExpr: String): Unit = {
    try {
      super.assertResult(expected)(actual)
    } catch {
      case ex: TestFailedException =>
        // Print some helpful diagnostics in the case of failure
        alert("Expected SELECT \"" + selectExpr + "\" to select the schema\n" +
          indent(StructType(expected :: Nil).treeString) +
          indent("but it actually selected\n") +
          indent(StructType(actual :: Nil).treeString) +
          indent("Note that expected.dataType.sameType(actual.dataType) = " +
          expected.dataType.sameType(actual.dataType)))
        throw ex
    }
  }

  // Test that the given SELECT expressions prune the test schema to the single-column schema
  // defined by the given field
  private def testSelect(inputSchema: StructType, selectExprs: String*)
                        (expected: StructField): Unit = {
    test(s"SELECT ${selectExprs.map(s => s""""$s"""").mkString(", ")} should select the schema\n" +
      indent(StructType(expected :: Nil).treeString)) {
      for (selectExpr <- selectExprs) {
        assertSelect(selectExpr, expected, inputSchema)
      }
    }
  }

  private def assertSelect(expr: String, expected: StructField, inputSchema: StructType): Unit = {
    val relation = LocalRelation(inputSchema.toAttributes)
    unapplySelect(expr, relation) match {
      case Some(field) =>
        assertResult(expected)(field)(expr)
      case None =>
        val failureMessage =
          "Failed to select a field from " + expr + ". " +
          "Expected:\n" +
          StructType(expected :: Nil).treeString
        fail(failureMessage)
    }
  }

  private def unapplySelect(expr: String, relation: LocalRelation) = {
    val parsedExpr = parseAsCatalystExpression(Seq(expr)).head
    val select = relation.select(parsedExpr)
    val analyzed = getAnalyzer.execute(select)
    SelectedField.unapply(analyzed.expressions.head)
  }

  private def parseAsCatalystExpression(exprs: Seq[String]) = {
    exprs.map(CatalystSqlParser.parseExpression(_) match {
      case namedExpr: NamedExpression => namedExpr
    })
  }

  // Indent every line in `string` by four spaces
  private def indent(string: String) = string.replaceAll("(?m)^", "   ")
}
