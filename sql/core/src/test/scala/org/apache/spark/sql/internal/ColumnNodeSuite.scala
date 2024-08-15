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
package org.apache.spark.sql.internal

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{functions, Dataset}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.types.{IntegerType, LongType, Metadata, MetadataBuilder, StringType}

class ColumnNodeSuite extends SparkFunSuite {
  private val simpleUdf = functions.udf((i: Int) => i + 1)

  test("sql") {
    testSql(Literal(null), "NULL")
    testSql(Literal(10), "10")
    testSql(Literal(33L), "33L")
    testSql(Literal(55.toShort), "55S")
    testSql(Literal(3.14), "3.14")
    testSql(Literal(Double.NaN), "NaN")
    testSql(Literal(Double.NegativeInfinity), "-Infinity")
    testSql(Literal(Double.PositiveInfinity), "Infinity")
    testSql(Literal(3.9f), "3.9")
    testSql(Literal(Float.NaN), "NaN")
    testSql(Literal(Float.NegativeInfinity), "-Infinity")
    testSql(Literal(Float.PositiveInfinity), "Infinity")
    testSql(Literal("hello"), "'hello'")
    testSql(Literal("\\_'"), "'\\\\_\\''")
    testSql(Literal((1, 2)), "(1,2)")
    testSql(UnresolvedStar(None), "*")
    testSql(UnresolvedStar(Option("prefix")), "prefix.*")
    testSql(UnresolvedAttribute("a"), "a")
    testSql(SqlExpression("1 + 1"), "1 + 1")
    testSql(Alias(UnresolvedAttribute("b"), Seq("new_b")), "b AS new_b")
    testSql(Alias(UnresolvedAttribute("c"), Seq("x", "y", "z")), "c AS (x, y, z)")
    testSql(Cast(UnresolvedAttribute("c"), IntegerType), "CAST(c AS INT)")
    testSql(Cast(UnresolvedAttribute("d"), StringType, Option(Cast.Try)), "TRY_CAST(d AS STRING)")
    testSql(
      SortOrder(attribute("e"), SortOrder.Ascending, SortOrder.NullsLast),
      "e ASC NULLS LAST")
    testSql(
      SortOrder(attribute("f"), SortOrder.Ascending, SortOrder.NullsFirst),
      "f ASC NULLS FIRST")
    testSql(
      SortOrder(attribute("g"), SortOrder.Descending, SortOrder.NullsLast),
      "g DESC NULLS LAST")
    testSql(
      SortOrder(attribute("h"), SortOrder.Descending, SortOrder.NullsFirst),
      "h DESC NULLS FIRST")
    testSql(
      UnresolvedFunction("coalesce", Seq(Literal(null), UnresolvedAttribute("i"))),
      "coalesce(NULL, i)")
    val lambdaVariableX = new UnresolvedNamedLambdaVariable("x")
    val lambdaVariableY = new UnresolvedNamedLambdaVariable("y")
    testSql(
      UnresolvedFunction(
        "transform", Seq(
          UnresolvedAttribute("input"),
          LambdaFunction(
            UnresolvedFunction("adjust", Seq(lambdaVariableX, UnresolvedAttribute("b"))),
            Seq(lambdaVariableX)))),
      "transform(input, x -> adjust(x, b))")
    testSql(
      UnresolvedFunction(
        "transform", Seq(
          UnresolvedAttribute("input"),
          LambdaFunction(
            UnresolvedFunction(
              "combine",
              Seq(lambdaVariableX, lambdaVariableY, UnresolvedAttribute("b"))),
            Seq(lambdaVariableX, lambdaVariableY)))),
      "transform(input, (x, y) -> combine(x, y, b))")
    testSql(UnresolvedExtractValue(attribute("a", 2), attribute("b", 3)), "a[b]")
    testSql(UpdateFields(UnresolvedAttribute("struct"), "a"), "drop_field(struct, a)")
    testSql(
      UpdateFields(UnresolvedAttribute("struct"), "b", Option(Literal(10.toLong))),
      "update_field(struct, b, 10L)")
    testSql(CaseWhenOtherwise(
      Seq(UnresolvedAttribute("c1") -> UnresolvedAttribute("v1"))),
      "CASE WHEN c1 THEN v1 END")
    testSql(CaseWhenOtherwise(
      Seq(
        UnresolvedAttribute("c1") -> UnresolvedAttribute("v1"),
        UnresolvedAttribute("c2") -> UnresolvedAttribute("v2")),
      Option(Literal(25))),
      "CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE 25 END")
    val windowSpec = WindowSpec(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")),
      Seq(
        SortOrder(attribute("x"), SortOrder.Descending, SortOrder.NullsFirst),
        SortOrder(attribute("y"), SortOrder.Ascending, SortOrder.NullsFirst)))
    val reducedWindowSpec = windowSpec.copy(
      partitionColumns = windowSpec.partitionColumns.take(1),
      sortColumns = windowSpec.sortColumns.take(1))
    val window = Window(
      UnresolvedFunction("sum", Seq(UnresolvedAttribute("i"))),
      WindowSpec(Nil, Nil))
    testSql(window, "sum(i) OVER ()")
    testSql(
      window.copy(windowSpec = windowSpec.copy(sortColumns = Nil)),
      "sum(i) OVER (PARTITION BY a, b)")
    testSql(
      window.copy(windowSpec = windowSpec.copy(partitionColumns = Nil)),
      "sum(i) OVER (ORDER BY x DESC NULLS FIRST, y ASC NULLS FIRST)")
    testSql(
      window.copy(windowSpec = windowSpec),
      "sum(i) OVER (PARTITION BY a, b ORDER BY x DESC NULLS FIRST, y ASC NULLS FIRST)")
    testSql(
      window.copy(windowSpec = reducedWindowSpec),
      "sum(i) OVER (PARTITION BY a ORDER BY x DESC NULLS FIRST)")
    testSql(
      window.copy(windowSpec = reducedWindowSpec.copy(frame = Option(WindowFrame(
        WindowFrame.Row,
        WindowFrame.UnboundedPreceding,
        WindowFrame.CurrentRow)))),
      "sum(i) OVER (PARTITION BY a ORDER BY x DESC NULLS FIRST " +
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
    testSql(
      window.copy(windowSpec = reducedWindowSpec.copy(frame = Option(WindowFrame(
        WindowFrame.Range,
        WindowFrame.Value(Literal(-10)),
        WindowFrame.UnboundedFollowing)))),
      "sum(i) OVER (PARTITION BY a ORDER BY x DESC NULLS FIRST " +
        "RANGE BETWEEN -10 AND UNBOUNDED FOLLOWING)")
    testSql(InvokeInlineUserDefinedFunction(simpleUdf, Seq(UnresolvedAttribute("x"))), "UDF(x)")
    testSql(
      InvokeInlineUserDefinedFunction(simpleUdf.withName("smple"), Seq(UnresolvedAttribute("x"))),
      "smple(x)")
  }

  private def testSql(node: ColumnNode, expectedSql: String): Unit = {
    assert(node.sql == expectedSql)
  }

  test("normalization") {
    testNormalization(Literal(1))
    testNormalization(UnresolvedStar(Option("a.b"), planId = planId()))
    testNormalization(UnresolvedAttribute("x", planId = planId()))
    testNormalization(UnresolvedRegex(".*", planId = planId()))
    testNormalization(SqlExpression("1 + 1"))
    testNormalization(attribute("a"))
    testNormalization(Alias(attribute("a"), Seq("aa"), None))
    testNormalization(Cast(attribute("b"), IntegerType, Some(Cast.Try)))
    testNormalization(SortOrder(attribute("c"), SortOrder.Ascending, SortOrder.NullsLast))
    val lambdaVariable = UnresolvedNamedLambdaVariable("x")
    testNormalization(
      UnresolvedFunction(
        "transform", Seq(
          attribute("input", 331),
          LambdaFunction(
            UnresolvedFunction("adjust", Seq(lambdaVariable, attribute("b", 2))),
            Seq(lambdaVariable)))))
    testNormalization(UnresolvedExtractValue(attribute("b", 2), attribute("a", 8)))
    testNormalization(UpdateFields(attribute("struct", 4), "a", Option(attribute("a", 11))))
    testNormalization(CaseWhenOtherwise(
      Seq(
        attribute("c1", 5) -> attribute("v1", 2),
        attribute("c2", 3) -> attribute("v2", 4)),
      Option(attribute("v2", 5))))
    testNormalization(Window(
      UnresolvedFunction("sum", Seq(attribute("a")), isInternal = true, isDistinct = true),
      WindowSpec(
        Seq(attribute("b", 2)),
        Seq(SortOrder(attribute("c", 3), SortOrder.Descending, SortOrder.NullsFirst)),
        // Not a supported frame, just here for testing.
        Option(WindowFrame(
          WindowFrame.Range,
          WindowFrame.Value(attribute("d", 3)),
          WindowFrame.Value(attribute("e", 4)))))))
    testNormalization(InvokeInlineUserDefinedFunction(
      simpleUdf,
      Seq(attribute("a", 2))))
  }

  private def testNormalization(generate: => ColumnNode): Unit = {
    val a = CurrentOrigin.withOrigin(origin())(generate)
    val b = CurrentOrigin.withOrigin(origin())(generate)
    val c = try {
      createNormalized.set(true)
      CurrentOrigin.withOrigin(ColumnNode.NO_ORIGIN)(generate)
    } finally {
      createNormalized.set(false)
    }
    assert(a != a.normalized)
    assert(a.normalized eq a.normalized.normalized)
    assert(a != b)
    assert(a.normalized == b.normalized)
    assert(a.normalized == c)
  }

  private val createNormalized: ThreadLocal[Boolean] = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  private val idGenerator = new AtomicInteger()
  private def nextId: Int = idGenerator.incrementAndGet()

  private def origin(): Origin = Origin(line = Option(nextId))

  private def planId(): Option[Long] = {
    if (!createNormalized.get()) {
      Some(nextId.toLong)
    } else {
      None
    }
  }

  private def attribute(name: String, id: Long = 1): ColumnNode = {
    val metadata = if (!createNormalized.get()) {
      new MetadataBuilder()
        .putLong(Dataset.DATASET_ID_KEY, nextId)
        .build()
    } else {
      Metadata.empty
    }
    Wrapper(AttributeReference(name, LongType, metadata = metadata)(exprId = ExprId(id)))
  }
}
