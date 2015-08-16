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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.types._

case class TestFunction(
    children: Seq[Expression],
    inputTypes: Seq[AbstractDataType])
  extends Expression with ImplicitCastInputTypes with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = StringType
}

case class UnresolvedTestPlan() extends LeafNode {
  override lazy val resolved = false
  override def output: Seq[Attribute] = Nil
}

class AnalysisErrorSuite extends AnalysisTest {
  import TestRelations._

  def errorTest(
      name: String,
      plan: LogicalPlan,
      errorMessages: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    test(name) {
      assertAnalysisError(plan, errorMessages, caseSensitive)
    }
  }

  val dateLit = Literal.create(null, DateType)

  errorTest(
    "single invalid type, single arg",
    testRelation.select(TestFunction(dateLit :: Nil, IntegerType :: Nil).as('a)),
    "cannot resolve" :: "testfunction" :: "argument 1" :: "requires int type" ::
    "'null' is of date type" :: Nil)

  errorTest(
    "single invalid type, second arg",
    testRelation.select(
      TestFunction(dateLit :: dateLit :: Nil, DateType :: IntegerType :: Nil).as('a)),
    "cannot resolve" :: "testfunction" :: "argument 2" :: "requires int type" ::
    "'null' is of date type" :: Nil)

  errorTest(
    "multiple invalid type",
    testRelation.select(
      TestFunction(dateLit :: dateLit :: Nil, IntegerType :: IntegerType :: Nil).as('a)),
    "cannot resolve" :: "testfunction" :: "argument 1" :: "argument 2" ::
    "requires int type" :: "'null' is of date type" :: Nil)

  errorTest(
    "unresolved window function",
    testRelation2.select(
      WindowExpression(
        UnresolvedWindowFunction(
          "lead",
          UnresolvedAttribute("c") :: Nil),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as('window)),
    "lead" :: "window functions currently requires a HiveContext" :: Nil)

  errorTest(
    "too many generators",
    listRelation.select(Explode('list).as('a), Explode('list).as('b)),
    "only one generator" :: "explode" :: Nil)

  errorTest(
    "unresolved attributes",
    testRelation.select('abcd),
    "cannot resolve" :: "abcd" :: Nil)

  errorTest(
    "bad casts",
    testRelation.select(Literal(1).cast(BinaryType).as('badCast)),
  "cannot cast" :: Literal(1).dataType.simpleString :: BinaryType.simpleString :: Nil)

  errorTest(
    "sorting by unsupported column types",
    listRelation.orderBy('list.asc),
    "sort" :: "type" :: "array<int>" :: Nil)

  errorTest(
    "non-boolean filters",
    testRelation.where(Literal(1)),
    "filter" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "non-boolean join conditions",
    testRelation.join(testRelation, condition = Some(Literal(1))),
    "condition" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "missing group by",
    testRelation2.groupBy('a)('b),
    "'b'" :: "group by" :: Nil
  )

  errorTest(
    "ambiguous field",
    nestedRelation.select($"top.duplicateField"),
    "Ambiguous reference to fields" :: "duplicateField" :: Nil,
    caseSensitive = false)

  errorTest(
    "ambiguous field due to case insensitivity",
    nestedRelation.select($"top.differentCase"),
    "Ambiguous reference to fields" :: "differentCase" :: "differentcase" :: Nil,
    caseSensitive = false)

  errorTest(
    "missing field",
    nestedRelation2.select($"top.c"),
    "No such struct field" :: "aField" :: "bField" :: "cField" :: Nil,
    caseSensitive = false)

  errorTest(
    "catch all unresolved plan",
    UnresolvedTestPlan(),
    "unresolved" :: Nil)

  errorTest(
    "union with unequal number of columns",
    testRelation.unionAll(testRelation2),
    "union" :: "number of columns" :: testRelation2.output.length.toString ::
      testRelation.output.length.toString :: Nil)

  errorTest(
    "intersect with unequal number of columns",
    testRelation.intersect(testRelation2),
    "intersect" :: "number of columns" :: testRelation2.output.length.toString ::
      testRelation.output.length.toString :: Nil)

  errorTest(
    "except with unequal number of columns",
    testRelation.except(testRelation2),
    "except" :: "number of columns" :: testRelation2.output.length.toString ::
      testRelation.output.length.toString :: Nil)

  errorTest(
    "SPARK-9955: correct error message for aggregate",
    // When parse SQL string, we will wrap aggregate expressions with UnresolvedAlias.
    testRelation2.where('bad_column > 1).groupBy('a)(UnresolvedAlias(max('b))),
    "cannot resolve 'bad_column'" :: Nil)

  test("SPARK-6452 regression test") {
    // CheckAnalysis should throw AnalysisException when Aggregate contains missing attribute(s)
    val plan =
      Aggregate(
        Nil,
        Alias(Sum(AttributeReference("a", IntegerType)(exprId = ExprId(1))), "b")() :: Nil,
        LocalRelation(
          AttributeReference("a", IntegerType)(exprId = ExprId(2))))

    assert(plan.resolved)

    assertAnalysisError(plan, "resolved attribute(s) a#1 missing from a#2" :: Nil)
  }

  test("error test for self-join") {
    val join = Join(testRelation, testRelation, Inner, None)
    val error = intercept[AnalysisException] {
      SimpleAnalyzer.checkAnalysis(join)
    }
    assert(error.message.contains("Failure when resolving conflicting references in Join"))
    assert(error.message.contains("Conflicting attributes"))
  }

  test("aggregation can't work on binary and map types") {
    val plan =
      Aggregate(
        AttributeReference("a", BinaryType)(exprId = ExprId(2)) :: Nil,
        Alias(Sum(AttributeReference("b", IntegerType)(exprId = ExprId(1))), "c")() :: Nil,
        LocalRelation(
          AttributeReference("a", BinaryType)(exprId = ExprId(2)),
          AttributeReference("b", IntegerType)(exprId = ExprId(1))))

    assertAnalysisError(plan,
      "binary type expression a cannot be used in grouping expression" :: Nil)

    val plan2 =
      Aggregate(
        AttributeReference("a", MapType(IntegerType, StringType))(exprId = ExprId(2)) :: Nil,
        Alias(Sum(AttributeReference("b", IntegerType)(exprId = ExprId(1))), "c")() :: Nil,
        LocalRelation(
          AttributeReference("a", MapType(IntegerType, StringType))(exprId = ExprId(2)),
          AttributeReference("b", IntegerType)(exprId = ExprId(1))))

    assertAnalysisError(plan2,
      "map type expression a cannot be used in grouping expression" :: Nil)
  }

  test("Join can't work on binary and map types") {
    val plan =
      Join(
        LocalRelation(
          AttributeReference("a", BinaryType)(exprId = ExprId(2)),
          AttributeReference("b", IntegerType)(exprId = ExprId(1))),
        LocalRelation(
          AttributeReference("c", BinaryType)(exprId = ExprId(4)),
          AttributeReference("d", IntegerType)(exprId = ExprId(3))),
        Inner,
        Some(EqualTo(AttributeReference("a", BinaryType)(exprId = ExprId(2)),
          AttributeReference("c", BinaryType)(exprId = ExprId(4)))))

    assertAnalysisError(plan, "binary type expression a cannot be used in join conditions" :: Nil)

    val plan2 =
      Join(
        LocalRelation(
          AttributeReference("a", MapType(IntegerType, StringType))(exprId = ExprId(2)),
          AttributeReference("b", IntegerType)(exprId = ExprId(1))),
        LocalRelation(
          AttributeReference("c", MapType(IntegerType, StringType))(exprId = ExprId(4)),
          AttributeReference("d", IntegerType)(exprId = ExprId(3))),
        Inner,
        Some(EqualTo(AttributeReference("a", MapType(IntegerType, StringType))(exprId = ExprId(2)),
          AttributeReference("c", MapType(IntegerType, StringType))(exprId = ExprId(4)))))

    assertAnalysisError(plan2, "map type expression a cannot be used in join conditions" :: Nil)
  }
}
