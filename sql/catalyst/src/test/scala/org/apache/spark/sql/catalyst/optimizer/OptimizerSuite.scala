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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, ArrayCompact, AttributeReference, CreateArray, CreateStruct, IntegerLiteral, Literal, MapFromEntries, Multiply, NamedExpression, Remainder}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * A dummy optimizer rule for testing that decrements integer literals until 0.
 */
object DecrementLiterals extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case IntegerLiteral(i) if i > 0 => Literal(i - 1)
  }
}

class OptimizerSuite extends PlanTest {
  test("Optimizer exceeds max iterations") {
    val iterations = 5
    val maxIterationsNotEnough = 3
    val maxIterationsEnough = 10
    val analyzed = Project(Alias(Literal(iterations), "attr")() :: Nil, OneRowRelation()).analyze

    withSQLConf(SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> maxIterationsNotEnough.toString) {
      val optimizer = new SimpleTestOptimizer() {
        override def defaultBatches: Seq[Batch] =
          Batch("test", fixedPoint,
            DecrementLiterals) :: Nil
      }

      val message1 = intercept[RuntimeException] {
        optimizer.execute(analyzed)
      }.getMessage
      assert(message1.startsWith(s"Max iterations ($maxIterationsNotEnough) reached for batch " +
        s"test, please set '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}' to a larger value."))

      withSQLConf(SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> maxIterationsEnough.toString) {
        try {
          optimizer.execute(analyzed)
        } catch {
          case ex: AnalysisException
            if ex.getMessage.contains(SQLConf.OPTIMIZER_MAX_ITERATIONS.key) =>
              fail("optimizer.execute should not reach max iterations.")
        }
      }

      val message2 = intercept[RuntimeException] {
        optimizer.execute(analyzed)
      }.getMessage
      assert(message2.startsWith(s"Max iterations ($maxIterationsNotEnough) reached for batch " +
        s"test, please set '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}' to a larger value."))
    }
  }

  test("Optimizer per rule validation catches dangling references") {
    val analyzed = Project(Alias(Literal(10), "attr")() :: Nil,
      OneRowRelation()).analyze

    /**
     * A dummy optimizer rule for testing that dangling references are not allowed.
     */
    object DanglingReference extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
          Project(Alias(
            Add(AttributeReference("debug1", IntegerType, nullable = false)(),
            AttributeReference("debug2", IntegerType, nullable = false)()), "attr")() :: Nil,
            plan)
      }
    }

    val optimizer = new SimpleTestOptimizer() {
        override def defaultBatches: Seq[Batch] =
          Batch("test", FixedPoint(1),
            DanglingReference) :: Nil
    }
    val message1 = intercept[SparkException] {
        optimizer.execute(analyzed)
    }.getMessage
    assert(message1.contains("are dangling"))
  }

  test("Optimizer per rule validation catches invalid aggregation expressions") {
    val analyzed = LocalRelation(Symbol("a").long, Symbol("b").long)
      .select(Symbol("a"), Symbol("b")).analyze

    /**
     * A dummy optimizer rule for testing that a non grouping key reference
     * should be aggregated (under an AggregateFunction).
     */
    object InvalidAggregationReference extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions = outputExpressions
        // I.e INVALID: select a, b from T group by a
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing that a non grouping key reference
     * should be aggregated (under an AggregateFunction).
     */
    object InvalidAggregationReference2 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions = Alias(Literal(1L), "a")() :: outputExpressions.last :: Nil
        // I.e INVALID: select 1 as a, b from T group by a
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing that a non grouping key expression
     * should be aggregated (under an AggregateFunction).
     */
    object InvalidAggregationExpression extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions = outputExpressions.head ::
          Alias(Add(outputExpressions.last, Literal(1L)), "b")() :: Nil
        // I.e INVALID: a, select b + 1 as b from T group by a
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing that a non grouping key expression
     * should be aggregated (under an AggregateFunction).
     */
    object InvalidAggregationExpression2 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions = Alias(Literal(1L), "a")() ::
          Alias(Remainder(outputExpressions.last, outputExpressions.head), "b")() :: Nil
        // I.e INVALID: select 1 as a, b % a as b from T group by a
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing that a non grouping key expression
     * should be aggregated (under an AggregateFunction).
     */
    object InvalidAggregationExpression3 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions = Alias(Literal(1L), "a")() ::
          Alias(Multiply(outputExpressions.head,
            Sum(outputExpressions.head).toAggregateExpression()), "b")() :: Nil
        // I.e VALID: select 1 as a, a*sum(a) as b from T group by a
        // analyze() should not fail.
        val goodAggregate =
          Aggregate(groupingExpressions, aggregateExpressions, plan)
            .analyze.asInstanceOf[Aggregate]
        assert(goodAggregate.analyzed)
        // I.e INVALID: select 1 as a, a*sum(a) as b from T group by b
        // Rule-validation should catch this.
       Aggregate(outputExpressions.last :: Nil, goodAggregate.aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing valid aggregate expression
     */
    object ValidAggregationExpression extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions : Seq[NamedExpression] = outputExpressions.head ::
          Alias(Add(outputExpressions.head, Literal(1L)), "b")() :: Nil
        // I.e VALID: select a, a + 1 as b from T group by a
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing another valid aggregate expression
     */
    object ValidAggregationExpression2 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = Add(outputExpressions.head, Literal(1L)) :: Nil
        val aggregateExpressions : Seq[NamedExpression] = Alias(Literal(1L), "a")() ::
          Alias(Add(outputExpressions.head, Literal(1L)), "b")() :: Nil
        // I.e VALID: select 1 as a, a + 1 as b from T group by a + 1
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing another valid aggregate expression
     */
    object ValidAggregationExpression3 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = Add(outputExpressions.head, Literal(1L)) :: Nil
        val aggregateExpressions : Seq[NamedExpression] = Alias(Literal(1L), "a")() ::
          Alias(Add(Add(outputExpressions.head, Literal(1L)), Literal(1L)), "b")() :: Nil
        // I.e VALID: select 1 as a, a + 1 + 1 as b from T group by a + 1
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing another valid aggregate expression
     */
    object ValidAggregationExpression4 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = Add(outputExpressions.head, Literal(1L)) :: Nil
        val aggregateExpressions : Seq[NamedExpression] = Alias(Literal(1L), "a")() ::
          Alias(Sum(outputExpressions.last).toAggregateExpression(), "b")() :: Nil
        // I.e VALID: select 1 as a, sum(b) as b from T group by a + 1
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing another valid aggregate expression
     */
    object ValidAggregationExpression5 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = outputExpressions.head :: Nil
        val aggregateExpressions : Seq[NamedExpression] = Alias(Literal(1L), "a")() ::
          Alias(Sum(outputExpressions.head).toAggregateExpression(), "b")() :: Nil
        // I.e VALID: select 1 as a, sum(a) as b from T group by a
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing another valid aggregate expression
     */
    object ValidAggregationExpression6 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = Remainder(outputExpressions.head, Literal(2L)) :: Nil
        val aggregateExpressions : Seq[NamedExpression] = Alias(Literal(1L), "a")() ::
          Alias(Sum(outputExpressions.head).toAggregateExpression(), "b")() :: Nil
        // I.e VALID: select 1 as a, sum(a) as b from T group by a % 2
        Aggregate(groupingExpressions, aggregateExpressions, plan)
      }
    }

    /**
     * A dummy optimizer rule for testing another valid aggregate expression
     */
    object ValidAggregationExpression7 extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = {
        val outputExpressions = plan.output
        val groupingExpressions = Remainder(outputExpressions.head, Literal(2L)) :: Nil
        val aggregateExpressions : Seq[NamedExpression] = Alias(Literal(1L), "a")() ::
          Alias(Add(Sum(outputExpressions.head).toAggregateExpression(),
            groupingExpressions.head), "b")() :: Nil
        // I.e VALID: 1 as a, select sum(a)*(a % 2) as b from T group by a % 2
        Aggregate(groupingExpressions, aggregateExpressions, plan).analyze
      }
    }

    // Valid rules do not trigger exceptions.
    Seq(ValidAggregationExpression, ValidAggregationExpression2,
      ValidAggregationExpression3, ValidAggregationExpression4,
      ValidAggregationExpression5, ValidAggregationExpression6,
      ValidAggregationExpression7).map { r =>
      val optimizer = new SimpleTestOptimizer() {
        override def defaultBatches: Seq[Batch] =
          Batch("test", FixedPoint(1), r) :: Nil
      }
      assert(optimizer.execute(analyzed).resolved)
    }

    // Invalid rules trigger exceptions.
    Seq(InvalidAggregationReference, InvalidAggregationReference2,
      InvalidAggregationExpression, InvalidAggregationExpression2,
      InvalidAggregationExpression3).map { r =>
      val optimizer = new SimpleTestOptimizer() {
        override def defaultBatches: Seq[Batch] =
          Batch("test", FixedPoint(1), r) :: Nil
      }
      val message1 = intercept[SparkException] {
        optimizer.execute(analyzed)
      }.getMessage
      assert(message1.contains("not a valid aggregate expression"))
    }
  }

  test("array compact contains null") {
    val optimizer = new SimpleTestOptimizer() {
      override def defaultBatches: Seq[Batch] =
        Batch("test", fixedPoint,
          ReplaceExpressions) :: Nil
    }

    val array1 = ArrayCompact(CreateArray(Literal(1) :: Literal.apply(null) :: Nil, false))
    val plan1 = Project(Alias(array1, "arr")() :: Nil, OneRowRelation()).analyze
    val optimized1 = optimizer.execute(plan1)
     assert(plan1.schema === optimized1.schema)

    val struct = CreateStruct(Literal(1) :: Literal(2) :: Nil)
    val array2 = ArrayCompact(CreateArray(struct :: Literal.apply(null) :: Nil, false))
    val plan2 = Project(Alias(MapFromEntries(array2), "map")() :: Nil, OneRowRelation()).analyze
    val optimized2 = optimizer.execute(plan2)
    assert(plan2.schema === optimized2.schema)
  }
}
