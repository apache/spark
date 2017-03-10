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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

class SelectedFieldSuite extends SparkFunSuite {
  // The test schema as a tree string, i.e. `schema.treeString`
  // root
  //  |-- col1: string (nullable = true)
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
  //  |    |-- field5: array (nullable = true)
  //  |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |-- subfield1: struct (nullable = true)
  //  |    |    |    |    |-- subsubfield1: integer (nullable = true)
  //  |    |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |    |    |-- subfield2: struct (nullable = true)
  //  |    |    |    |    |-- subsubfield1: struct (nullable = true)
  //  |    |    |    |    |    |-- subsubsubfield1: string (nullable = true)
  //  |    |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |    |-- field6: struct (nullable = true)
  //  |    |    |-- subfield1: string (nullable = true)
  //  |    |    |-- subfield2: string (nullable = true)
  //  |    |-- field7: struct (nullable = true)
  //  |    |    |-- subfield1: struct (nullable = true)
  //  |    |    |    |-- subsubfield1: integer (nullable = true)
  //  |    |    |    |-- subsubfield2: integer (nullable = true)
  //  |-- col3: array (nullable = true)
  //  |    |-- element: struct (containsNull = true)
  //  |    |    |-- field1: struct (nullable = true)
  //  |    |    |    |-- subfield1: integer (nullable = true)
  //  |    |    |    |-- subfield2: integer (nullable = true)
  private val schema =
    StructType(
      StructField("col1", StringType) ::
      StructField("col2", StructType(
        StructField("field1", IntegerType) ::
        StructField("field2", ArrayType(IntegerType, false)) ::
        StructField("field3", ArrayType(StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) ::
          StructField("subfield3", ArrayType(IntegerType)) :: Nil)), false) ::
        StructField("field4", MapType(StringType, StructType(
          StructField("subfield1", IntegerType) :: Nil), false)) ::
        StructField("field5", ArrayType(StructType(
          StructField("subfield1", StructType(
            StructField("subsubfield1", IntegerType) ::
            StructField("subsubfield2", IntegerType) :: Nil)) ::
          StructField("subfield2", StructType(
            StructField("subsubfield1", StructType(
              StructField("subsubsubfield1", StringType) :: Nil)) ::
            StructField("subsubfield2", IntegerType) :: Nil)) :: Nil))) ::
        StructField("field6", StructType(
          StructField("subfield1", StringType) ::
          StructField("subfield2", StringType) :: Nil)) ::
        StructField("field7", StructType(
          StructField("subfield1", StructType(
            StructField("subsubfield1", IntegerType) ::
            StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)) :: Nil)) ::
     StructField("col3", ArrayType(StructType(
       StructField("field1", StructType(
         StructField("subfield1", IntegerType) ::
         StructField("subfield2", IntegerType) :: Nil)) :: Nil))) :: Nil)

  private val testRelation = LocalRelation(schema.toAttributes)

  test("should not match an attribute reference") {
    assertResult(None)(unapplySelect("col1"))
    assertResult(None)(unapplySelect("col1 as foo"))
    assertResult(None)(unapplySelect("col2"))
  }

  test("should extract a field from a GetArrayItem") {
    val expr1 = "col2.field2[0] as foo"
    unapplySelect(expr1) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field2", ArrayType(IntegerType, false)) :: Nil)))(field)(expr1)
      case None => fail
    }

    val expr2 = "col2.field3[0] as foo"
    unapplySelect(expr2) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field3", ArrayType(StructType(
              StructField("subfield1", IntegerType) ::
              StructField("subfield2", IntegerType) ::
              StructField("subfield3", ArrayType(IntegerType)) ::
              Nil)), false) :: Nil)))(field)(expr2)
      case None => fail
    }
  }

  test("should extract a field of atomic type from a GetArrayStructFields") {
    val expr = "col2.field3.subfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        val expected =
          StructField("col2", StructType(
            StructField("field3", ArrayType(StructType(
              StructField("subfield1", IntegerType) :: Nil)), false) :: Nil))
        assertResult(expected)(field)(expr)
      case None => fail
    }
  }

  test("should extract a field of array type from a GetArrayStructFields") {
    val expr = "col2.field3.subfield3"
    unapplySelect(expr) match {
      case Some(field) =>
        val expected =
          StructField("col2", StructType(
            StructField("field3", ArrayType(StructType(
              StructField("subfield3", ArrayType(IntegerType)) :: Nil)), false) :: Nil))
        assertResult(expected)(field)(expr)
      case None => fail
    }
  }

  test("should extract a field of struct type from a GetArrayStructFields") {
    val expr = "col2.field5.subfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        val expected =
          StructField("col2", StructType(
            StructField("field5", ArrayType(StructType(
              StructField("subfield1", StructType(
                StructField("subsubfield1", IntegerType) ::
                StructField("subsubfield2", IntegerType) :: Nil)) :: Nil))) :: Nil))
        assertResult(expected)(field)(expr)
      case None => fail
    }
  }

  test("blah") {
    val expr = "col3.field1.subfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        val expected =
          StructField("col3", ArrayType(StructType(
            StructField("field1", StructType(
              StructField("subfield1", IntegerType) :: Nil)) :: Nil)))
        assertResult(expected)(field)(expr)
      case None => fail
    }
  }

  test("should extract a field from a field of a GetArrayStructFields") {
    val expr = "col2.field5.subfield1.subsubfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        val expected =
          StructField("col2", StructType(
            StructField("field5", ArrayType(StructType(
              StructField("subfield1", StructType(
                StructField("subsubfield1", IntegerType) :: Nil)) :: Nil))) :: Nil))
        assertResult(expected)(field)(expr)
      case None => fail
    }
  }

  test("should extract a field from a field from a field of a GetArrayStructFields") {
    val expr = "col2.field5.subfield2.subsubfield1.subsubsubfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        val expected =
          StructField("col2", StructType(
            StructField("field5", ArrayType(StructType(
              StructField("subfield2", StructType(
                StructField("subsubfield1", StructType(
                  StructField("subsubsubfield1", StringType) :: Nil)) :: Nil)) :: Nil))) ::
            Nil))
        assertResult(expected)(field)(expr)
      case None => fail
    }
  }

  test("should extract a field from a GetMapValue") {
    val expr = "col2.field4['foo'] as foo"
    unapplySelect(expr) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field4", MapType(StringType, StructType(
              StructField("subfield1", IntegerType) :: Nil), false)) :: Nil)))(field)(expr)
      case None => fail
    }
  }

  test("should extract a field from a GetStructField") {
    val expr = "col2.field1"
    unapplySelect(expr) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field1", IntegerType) :: Nil)))(field)(expr)
      case None => fail
    }
  }

  test("should extract a field of type StructType from a GetStructField") {
    val expr = "col2.field6"
    unapplySelect(expr) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field6", StructType(
              StructField("subfield1", StringType) ::
              StructField("subfield2", StringType) :: Nil)) :: Nil)))(field)(expr)
      case None => fail
    }
  }

  test("should extract a subfield of type StructType from a field from a GetStructField") {
    val expr = "col2.field7.subfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field7", StructType(
              StructField("subfield1", StructType(
                StructField("subsubfield1", IntegerType) ::
                StructField("subsubfield2", IntegerType) :: Nil)) :: Nil)) :: Nil)))(field)(expr)
      case None => fail
    }
  }

  test("should extract a subfield from a field from a GetStructField") {
    val expr = "col2.field6.subfield1"
    unapplySelect(expr) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field6", StructType(
              StructField("subfield1", StringType) :: Nil)) :: Nil)))(field)(expr)
      case None => fail
    }
  }

  test("should extract an array field from a GetStructField") {
    val expr = "col2.field2"
    unapplySelect(expr) match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field2", ArrayType(IntegerType, false)) :: Nil)))(field)(expr)
      case None => fail
    }
  }

  def assertResult(expected: StructField)(actual: StructField)(expr: String): Unit = {
    try {
      super.assertResult(expected)(actual)
    } catch {
      case ex: TestFailedException =>
        println("For " + expr)
        println("Expected:")
        println(StructType(expected :: Nil).treeString)
        println("Actual:")
        println(StructType(actual :: Nil).treeString)
        println("expected.dataType.sameType(actual.dataType) = " +
          expected.dataType.sameType(actual.dataType))
        throw ex
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
}
