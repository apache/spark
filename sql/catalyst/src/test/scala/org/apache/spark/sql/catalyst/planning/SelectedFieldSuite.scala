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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

class SelectedFieldSuite extends SparkFunSuite {
  private val schema =
    StructType(
      StructField("col1", StringType) ::
      StructField("col2", StructType(
        StructField("field1", IntegerType) ::
        StructField("field2", ArrayType(IntegerType, false)) ::
        StructField("field3", ArrayType(StructType(
          StructField("subfield1", IntegerType) ::
          StructField("subfield2", IntegerType) :: Nil)), false) ::
        StructField("field4", MapType(StringType, StructType(
          StructField("subfield1", IntegerType) :: Nil), false)) :: Nil)) :: Nil)

  private val testRelation = LocalRelation(schema.toAttributes)

  test("should not match an attribute reference") {
    assertResult(None)(unapplySelect("col1"))
    assertResult(None)(unapplySelect("col1 as foo"))
    assertResult(None)(unapplySelect("col2"))
  }

  test("should extract a field from a GetArrayItem") {
    unapplySelect("col2.field2[0] as foo") match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field2", ArrayType(IntegerType, false)) :: Nil)))(field)
      case None => fail
    }

    unapplySelect("col2.field3[0] as foo") match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
              StructField("field3", ArrayType(StructType(
                StructField("subfield1", IntegerType) ::
                StructField("subfield2", IntegerType) :: Nil)), false) :: Nil)))(field)
      case None => fail
    }
  }

  test("should extract a field from a GetArrayStructFields") {
    unapplySelect("col2.field3.subfield1") match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field3", ArrayType(StructType(
              StructField("subfield1", IntegerType) :: Nil)), false) :: Nil)))(field)
      case None => fail
    }
  }

  test("should extract a field from a GetMapValue") {
    unapplySelect("col2.field4['foo'] as foo") match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field4", MapType(StringType, StructType(
              StructField("subfield1", IntegerType) :: Nil), false)) :: Nil)))(field)
      case None => fail
    }
  }

  test("should extract a field from a GetStructField") {
    unapplySelect("col2.field1") match {
      case Some(field) =>
        assertResult(
          StructField("col2", StructType(
            StructField("field1", IntegerType) :: Nil)))(field)
      case None => fail
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
