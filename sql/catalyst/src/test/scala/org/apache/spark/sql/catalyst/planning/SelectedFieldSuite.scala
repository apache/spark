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

package org.apache.spark.sql.catalyst.planning

import org.scalatest.BeforeAndAfterAll
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
class SelectedFieldSuite extends SparkFunSuite with BeforeAndAfterAll {
  // The test schema as a tree string, i.e. `schema.treeString`
  // root
  //  |-- col1: string (nullable = false)
  //  |-- col2: struct (nullable = true)
  //  |    |-- field1: integer (nullable = true)
  //  |    |-- field2: array (nullable = true)
  //  |    |    |-- element: integer (containsNull = false)
  //  |    |-- field3: array (nullable = false)
  //  |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  //  |    |    |    |-- subfield3: array (nullable = true)
  //  |    |    |    |    |-- element: integer (containsNull = true)
  //  |    |-- field4: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: struct (valueContainsNull = false)
  //  |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |-- subfield2: array (nullable = true)
  //  |    |    |    |    |-- element: integer (containsNull = false)
  //  |    |-- field5: array (nullable = false)
  //  |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |-- subfield1: struct (nullable = false)
  //  |    |    |    |    |-- subsubfield1: integer (nullable = true)
  //  |    |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |    |    |-- subfield2: struct (nullable = true)
  //  |    |    |    |    |-- subsubfield1: struct (nullable = true)
  //  |    |    |    |    |    |-- subsubsubfield1: string (nullable = true)
  //  |    |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |-- field6: struct (nullable = true)
  //  |    |    |-- subfield1: string (nullable = false)
  //  |    |    |-- subfield2: string (nullable = true)
  //  |    |-- field7: struct (nullable = true)
  //  |    |    |-- subfield1: struct (nullable = true)
  //  |    |    |    |-- subsubfield1: integer (nullable = true)
  //  |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |-- field8: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: array (valueContainsNull = false)
  //  |    |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: array (nullable = true)
  //  |    |    |    |    |    |-- element: integer (containsNull = false)
  //  |    |-- field9: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: integer (valueContainsNull = false)
  //  |-- col3: array (nullable = false)
  //  |    |-- element: struct (containsNull = false)
  //  |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = false)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  //  |    |    |-- field2: map (nullable = true)
  //  |    |    |    |-- key: string
  //  |    |    |    |-- value: integer (valueContainsNull = false)
  //  |-- col4: map (nullable = false)
  //  |    |-- key: string
  //  |    |-- value: struct (valueContainsNull = false)
  //  |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = false)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  //  |    |    |-- field2: map (nullable = true)
  //  |    |    |    |-- key: string
  //  |    |    |    |-- value: integer (valueContainsNull = false)
  //  |-- col5: array (nullable = true)
  //  |    |-- element: map (containsNull = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: struct (valueContainsNull = false)
  //  |    |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: integer (nullable = true)
  //  |-- col6: map (nullable = true)
  //  |    |-- key: string
  //  |    |-- value: array (valueContainsNull = true)
  //  |    |    |-- element: struct (containsNull = false)
  //  |    |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |    |-- subfield2: integer (nullable = true)
  //  |-- col7: array (nullable = true)
  //  |    |-- element: struct (containsNull = true)
  //  |    |    |-- field1: integer (nullable = false)
  //  |    |    |-- field2: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = false)
  //  |    |    |-- field3: array (nullable = true)
  //  |    |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |    |-- subfield1: integer (nullable = false)
  //  |-- col8: array (nullable = true)
  //  |    |-- element: struct (containsNull = true)
  //  |    |    |-- field1: array (nullable = false)
  //  |    |    |    |-- element: integer (containsNull = false)
  private val schema =
    StructType(
      StructField("col1", StringType, nullable = false) ::
      StructField("col2", StructType(
        StructField("field1", IntegerType) ::
        StructField("field2", ArrayType(IntegerType, containsNull = false)) ::
        StructField("field3", ArrayType(StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) ::
          StructField("subfield3", ArrayType(IntegerType)) :: Nil)), nullable = false) ::
        StructField("field4", MapType(StringType, StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", ArrayType(IntegerType, containsNull = false)) :: Nil), valueContainsNull = false)) ::
        StructField("field5", ArrayType(StructType(
          StructField("subfield1", StructType(
            StructField("subsubfield1", IntegerType) ::
            StructField("subsubfield2", IntegerType) :: Nil), nullable = false) ::
          StructField("subfield2", StructType(
            StructField("subsubfield1", StructType(
              StructField("subsubsubfield1", StringType) :: Nil)) ::
            StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)), nullable = false) ::
        StructField("field6", StructType(
          StructField("subfield1", StringType, nullable = false) ::
          StructField("subfield2", StringType) :: Nil)) ::
        StructField("field7", StructType(
          StructField("subfield1", StructType(
            StructField("subsubfield1", IntegerType) ::
            StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)) ::
        StructField("field8", MapType(StringType, ArrayType(StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", ArrayType(IntegerType, containsNull = false)) :: Nil)), valueContainsNull = false)) ::
        StructField("field9", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil)) ::
     StructField("col3", ArrayType(StructType(
       StructField("field1", StructType(
         StructField("subfield1", IntegerType, nullable = false) ::
         StructField("subfield2", IntegerType) :: Nil)) ::
       StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil), containsNull = false), nullable = false) ::
     StructField("col4", MapType(StringType, StructType(
       StructField("field1", StructType(
         StructField("subfield1", IntegerType, nullable = false) ::
         StructField("subfield2", IntegerType) :: Nil)) ::
       StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil), valueContainsNull = false), nullable = false) ::
     StructField("col5", ArrayType(MapType(StringType, StructType(
       StructField("field1", StructType(
         StructField("subfield1", IntegerType) ::
         StructField("subfield2", IntegerType) :: Nil)) :: Nil), valueContainsNull = false))) ::
     StructField("col6", MapType(StringType, ArrayType(StructType(
       StructField("field1", StructType(
         StructField("subfield1", IntegerType) ::
         StructField("subfield2", IntegerType) :: Nil)) :: Nil), containsNull = false))) ::
     StructField("col7", ArrayType(StructType(
       StructField("field1", IntegerType, nullable = false) ::
       StructField("field2", StructType(
         StructField("subfield1", IntegerType, nullable = false) :: Nil)) ::
       StructField("field3", ArrayType(StructType(
         StructField("subfield1", IntegerType, nullable = false) :: Nil))) :: Nil))) ::
     StructField("col8", ArrayType(StructType(
       StructField("field1", ArrayType(IntegerType, containsNull = false), nullable = false) :: Nil))) :: Nil)

  private val testRelation = LocalRelation(schema.toAttributes)

  test("SelectedField should not match an attribute reference") {
    assertResult(None)(unapplySelect("col1"))
    assertResult(None)(unapplySelect("col1 as foo"))
    assertResult(None)(unapplySelect("col2"))
  }

  info("For a relation with schema\n" + indent(schema.treeString))

  testSelect("col2.field2", "col2.field2[0] as foo") {
    StructField("col2", StructType(
      StructField("field2", ArrayType(IntegerType, containsNull = false)) :: Nil))
  }

  testSelect("col2.field9", "col2.field9['foo'] as foo") {
    StructField("col2", StructType(
      StructField("field9", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil))
  }

  testSelect("col2.field3.subfield3", "col2.field3[0].subfield3 as foo",
      "col2.field3.subfield3[0] as foo", "col2.field3[0].subfield3[0] as foo") {
    StructField("col2", StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield3", ArrayType(IntegerType)) :: Nil)), nullable = false) :: Nil))
  }

  testSelect("col2.field3.subfield1") {
    StructField("col2", StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield1", IntegerType) :: Nil)), nullable = false) :: Nil))
  }

  testSelect("col2.field5.subfield1") {
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) ::
          StructField("subsubfield2", IntegerType) :: Nil), nullable = false) :: Nil)), nullable = false) :: Nil))
  }

  testSelect("col3.field1.subfield1") {
    StructField("col3", ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil)) :: Nil), containsNull = false), nullable = false)
  }

  testSelect("col3.field2['foo'] as foo") {
    StructField("col3", ArrayType(StructType(
      StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil), containsNull = false), nullable = false)
  }

  testSelect("col4['foo'].field1.subfield1 as foo") {
    StructField("col4", MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil)) :: Nil), valueContainsNull = false), nullable = false)
  }

  testSelect("col4['foo'].field2['bar'] as foo") {
    StructField("col4", MapType(StringType, StructType(
      StructField("field2", MapType(StringType, IntegerType, valueContainsNull = false)) :: Nil), valueContainsNull = false), nullable = false)
  }

  testSelect("col5[0]['foo'].field1.subfield1 as foo") {
    StructField("col5", ArrayType(MapType(StringType, StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) :: Nil)) :: Nil), valueContainsNull = false)))
  }

  testSelect("col6['foo'][0].field1.subfield1 as foo") {
    StructField("col6", MapType(StringType, ArrayType(StructType(
      StructField("field1", StructType(
        StructField("subfield1", IntegerType) :: Nil)) :: Nil), containsNull = false)))
  }

  testSelect("col2.field5.subfield1.subsubfield1") {
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) :: Nil), nullable = false) :: Nil)), nullable = false) :: Nil))
  }

  testSelect("col2.field5.subfield2.subsubfield1.subsubsubfield1") {
    StructField("col2", StructType(
      StructField("field5", ArrayType(StructType(
        StructField("subfield2", StructType(
          StructField("subsubfield1", StructType(
            StructField("subsubsubfield1", StringType) :: Nil)) :: Nil)) :: Nil)), nullable = false) :: Nil))
  }

  testSelect("col2.field4['foo'].subfield1 as foo") {
    StructField("col2", StructType(
      StructField("field4", MapType(StringType, StructType(
        StructField("subfield1", IntegerType) :: Nil), valueContainsNull = false)) :: Nil))
  }

  testSelect("col2.field4['foo'].subfield2 as foo", "col2.field4['foo'].subfield2[0] as foo") {
    StructField("col2", StructType(
      StructField("field4", MapType(StringType, StructType(
        StructField("subfield2", ArrayType(IntegerType, containsNull = false)) :: Nil), valueContainsNull = false)) :: Nil))
  }

  testSelect("col2.field8['foo'][0].subfield1 as foo") {
    StructField("col2", StructType(
      StructField("field8", MapType(StringType, ArrayType(StructType(
        StructField("subfield1", IntegerType) :: Nil)), valueContainsNull = false)) :: Nil))
  }

  testSelect("col2.field1") {
    StructField("col2", StructType(
      StructField("field1", IntegerType) :: Nil))
  }

  testSelect("col2.field6") {
    StructField("col2", StructType(
      StructField("field6", StructType(
        StructField("subfield1", StringType, nullable = false) ::
        StructField("subfield2", StringType) :: Nil)) :: Nil))
  }

  testSelect("col2.field7.subfield1") {
    StructField("col2", StructType(
      StructField("field7", StructType(
        StructField("subfield1", StructType(
          StructField("subsubfield1", IntegerType) ::
          StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)) :: Nil))
  }

  testSelect("col2.field6.subfield1") {
    StructField("col2", StructType(
      StructField("field6", StructType(
        StructField("subfield1", StringType, nullable = false) :: Nil)) :: Nil))
  }

  testSelect("col7.field1", "col7[0].field1 as foo", "col7.field1[0] as foo") {
    StructField("col7", ArrayType(StructType(
      StructField("field1", IntegerType, nullable = false) :: Nil)))
  }

  testSelect("col7.field2.subfield1") {
    StructField("col7", ArrayType(StructType(
      StructField("field2", StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil)) :: Nil)))
  }

  testSelect("col7.field3.subfield1") {
    StructField("col7", ArrayType(StructType(
      StructField("field3", ArrayType(StructType(
        StructField("subfield1", IntegerType, nullable = false) :: Nil))) :: Nil)))
  }

  testSelect("col8.field1", "col8[0].field1 as foo", "col8.field1[0] as foo", "col8[0].field1[0] as foo") {
    StructField("col8", ArrayType(StructType(
      StructField("field1", ArrayType(IntegerType, containsNull = false), nullable = false) :: Nil)))
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
  private def testSelect(selectExpr: String, otherSelectExprs: String*)(expected: StructField) {
    val selectExprs = selectExpr +: otherSelectExprs
    test(s"SELECT " + selectExprs.map(s => s""""$s"""").mkString(", ") + " should select the schema\n" +
        indent(StructType(expected :: Nil).treeString)) {
      for (selectExpr <- selectExprs) {
        assertSelect(selectExpr, expected)
      }
    }
  }

  private def assertSelect(expr: String, expected: StructField) = {
    unapplySelect(expr) match {
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

  private def unapplySelect(expr: String) = {
    val parsedExpr =
      CatalystSqlParser.parseExpression(expr) match {
        case namedExpr: NamedExpression => namedExpr
      }
    val select = testRelation.select(parsedExpr)
    val analyzed = select.analyze
    SelectedField.unapply(analyzed.expressions.head)
  }

  private def indent(string: String) = string.replaceAll("(?m)^", "   ")
}
