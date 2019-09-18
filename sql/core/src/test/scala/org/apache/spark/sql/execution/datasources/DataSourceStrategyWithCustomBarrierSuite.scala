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

package org.apache.spark.sql.execution.datasources

import org.junit.Assert

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Factorial, GreaterThan, IsNotNull, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{CustomBarrier, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.test.SharedSparkSession


class DataSourceStrategyWithCustomBarrierSuite extends PlanTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(
      """
        |CREATE TEMPORARY VIEW oneToTenFiltered
        |USING org.apache.spark.sql.sources.FilteredScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  test("failing case with CustomBarrier") {

    // Inserting the CustomBarrier logical operator to block some of the Spark's optimization rule.
    implicit class DslLogicalPlanCustomBarrier(val logicalPlan: LogicalPlan) {
      def customBarrier: LogicalPlan =
        CustomBarrier(logicalPlan)
    }

    // Custom optimization added to remove the CustomBarrier
    object RemoveCustomBarrier extends Rule[LogicalPlan] with PredicateHelper {
      def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
        case CustomBarrier(child) => child
      }
    }

    spark.sessionState.experimentalMethods.extraOptimizations ++= Seq(RemoveCustomBarrier)

    val logicalPlan = table("oneToTenFiltered").select('a as 'a1, 'b as 'b1, 'c as 'c1)
      .subquery('tmp1)
      .select(Factorial('a1) as 'a2, 'b1.attr, 'c1.attr)
      .subquery('tmp2)
      .where('b1 > 1)
      .select('a2.attr, 'b1.attr, 'c1.attr)
      .customBarrier
      .subquery('tmp3)
      .where('a2.attr > 5)
      .select('a2.attr, 'b1.attr, 'c1.attr)
      .subquery('tmp4)
      .select('b1.attr)

    val query = logicalPlanToSparkQuery(logicalPlan)

    // With the change in SPARK-29029, we have the substituted alias in Filter condition.
    // However, without SPARK-29029 this test case would throw java.util.NoSuchElementException.
    verifyPlan(query) {
      case ProjectExec(Seq(NamedExpression("b1", _)),
            FilterExec(And(
                        And(
                          IsNotNull(Alias(Factorial(NamedExpression("a", _)), "a2")),
                          GreaterThan(Alias(Factorial(NamedExpression("a", _)), "a2"), _)
                        ),
                        GreaterThan(NamedExpression("b", _), _)
                       ),
              _: RowDataSourceScanExec)) =>
    }
  }

  private def verifyPlan(df: DataFrame)(expected: PartialFunction[SparkPlan, Unit]): Unit = {
    val plan = df.queryExecution.sparkPlan

    expected.applyOrElse(plan, (_: SparkPlan) =>
      Assert.fail(s"Actual plan $plan did not match expected."))
  }
}
