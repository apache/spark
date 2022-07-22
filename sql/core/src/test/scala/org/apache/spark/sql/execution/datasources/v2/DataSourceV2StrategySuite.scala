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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

class DataSourceV2StrategySuite extends PlanTest with SharedSparkSession {
  val attrInts = Seq(
    $"cint".int,
    $"c.int".int,
    GetStructField($"a".struct(StructType(
      StructField("cstr", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 1, None),
    GetStructField($"a".struct(StructType(
      StructField("c.int", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 0, None),
    GetStructField($"a.b".struct(StructType(
      StructField("cstr1", StringType, nullable = true) ::
        StructField("cstr2", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 2, None),
    GetStructField($"a.b".struct(StructType(
      StructField("c.int", IntegerType, nullable = true) :: Nil)), 0, None),
    GetStructField(GetStructField($"a".struct(StructType(
      StructField("cstr1", StringType, nullable = true) ::
        StructField("b", StructType(StructField("cint", IntegerType, nullable = true) ::
          StructField("cstr2", StringType, nullable = true) :: Nil)) :: Nil)), 1, None), 0, None)
  ).zip(Seq(
    "cint",
    "`c.int`", // single level field that contains `dot` in name
    "a.cint", // two level nested field
    "a.`c.int`", // two level nested field, and nested level contains `dot`
    "`a.b`.cint", // two level nested field, and top level contains `dot`
    "`a.b`.`c.int`", // two level nested field, and both levels contain `dot`
    "a.b.cint" // three level nested field
  ))

  test("SPARK-39784: translate binary expression") { attrInts
    .foreach { case (attrInt, intColName) =>
      testTranslateFilter(EqualTo(attrInt, 1),
        Some(new Predicate("=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(EqualTo(1, attrInt),
        Some(new Predicate("=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(EqualNullSafe(attrInt, 1),
        Some(new Predicate("<=>", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(EqualNullSafe(1, attrInt),
        Some(new Predicate("<=>", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(GreaterThan(attrInt, 1),
        Some(new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(GreaterThan(1, attrInt),
        Some(new Predicate("<", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(LessThan(attrInt, 1),
        Some(new Predicate("<", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(LessThan(1, attrInt),
        Some(new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(GreaterThanOrEqual(attrInt, 1),
        Some(new Predicate(">=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(GreaterThanOrEqual(1, attrInt),
        Some(new Predicate("<=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(LessThanOrEqual(attrInt, 1),
        Some(new Predicate("<=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(LessThanOrEqual(1, attrInt),
        Some(new Predicate(">=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
    }
  }

  test("SPARK-36644: Push down boolean column filter") {
    testTranslateFilter($"col".boolean,
      Some(new Predicate("=", Array(FieldReference("col"), LiteralValue(true, BooleanType)))))
  }

  /**
   * Translate the given Catalyst [[Expression]] into data source V2 [[Predicate]]
   * then verify against the given [[Predicate]].
   */
  def testTranslateFilter(catalystFilter: Expression, result: Option[Predicate]): Unit = {
    assertResult(result) {
      DataSourceV2Strategy.translateFilterV2(catalystFilter)
    }
  }
}
