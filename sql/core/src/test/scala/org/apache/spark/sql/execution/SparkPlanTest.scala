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

package org.apache.spark.sql.execution

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, DataFrameHolder, Row, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Base class for writing tests for individual physical operators. For an example of how this
 * class's test helper methods can be used, see [[SortSuite]].
 */
private[sql] abstract class SparkPlanTest extends SparkFunSuite {
  protected def sqlContext: SQLContext

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    doCheckAnswer(
      input :: Nil,
      (plans: Seq[SparkPlan]) => planFunction(plans.head),
      expectedAnswer,
      sortAnswers)
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param left the left input data to be used.
   * @param right the right input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkAnswer2(
      left: DataFrame,
      right: DataFrame,
      planFunction: (SparkPlan, SparkPlan) => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    doCheckAnswer(
      left :: right :: Nil,
      (plans: Seq[SparkPlan]) => planFunction(plans(0), plans(1)),
      expectedAnswer,
      sortAnswers)
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts a sequence of input SparkPlans and uses them to
   *                     instantiate the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def doCheckAnswer(
      input: Seq[DataFrame],
      planFunction: Seq[SparkPlan] => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean = true): Unit = {
    SparkPlanTest.checkAnswer(input, planFunction, expectedAnswer, sortAnswers, sqlContext) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected def checkThatPlansAgree(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedPlanFunction: SparkPlan => SparkPlan,
      sortAnswers: Boolean = true): Unit = {
    SparkPlanTest.checkAnswer(
        input, planFunction, expectedPlanFunction, sortAnswers, sqlContext) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}

/**
 * Helper methods for writing tests of individual physical operators.
 */
object SparkPlanTest {

  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   */
  def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedPlanFunction: SparkPlan => SparkPlan,
      sortAnswers: Boolean,
      sqlContext: SQLContext): Option[String] = {

    val outputPlan = planFunction(input.queryExecution.sparkPlan)
    val expectedOutputPlan = expectedPlanFunction(input.queryExecution.sparkPlan)

    val expectedAnswer: Seq[Row] = try {
      executePlan(expectedOutputPlan, sqlContext)
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan to calculate expected answer:
             | $expectedOutputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    val actualAnswer: Seq[Row] = try {
      executePlan(outputPlan, sqlContext)
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan:
             | $outputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    SQLTestUtils.compareAnswers(actualAnswer, expectedAnswer, sortAnswers).map { errorMessage =>
      s"""
         | Results do not match.
         | Actual result Spark plan:
         | $outputPlan
         | Expected result Spark plan:
         | $expectedOutputPlan
         | $errorMessage
       """.stripMargin
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  def checkAnswer(
      input: Seq[DataFrame],
      planFunction: Seq[SparkPlan] => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean,
      sqlContext: SQLContext): Option[String] = {

    val outputPlan = planFunction(input.map(_.queryExecution.sparkPlan))

    val sparkAnswer: Seq[Row] = try {
      executePlan(outputPlan, sqlContext)
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan:
             | $outputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    SQLTestUtils.compareAnswers(sparkAnswer, expectedAnswer, sortAnswers).map { errorMessage =>
      s"""
         | Results do not match for Spark plan:
         | $outputPlan
         | $errorMessage
       """.stripMargin
    }
  }

  private def executePlan(outputPlan: SparkPlan, sqlContext: SQLContext): Seq[Row] = {
    // A very simple resolver to make writing tests easier. In contrast to the real resolver
    // this is always case sensitive and does not try to handle scoping or complex type resolution.
    val resolvedPlan = sqlContext.prepareForExecution.execute(
      outputPlan transform {
        case plan: SparkPlan =>
          val inputMap = plan.children.flatMap(_.output).map(a => (a.name, a)).toMap
          plan transformExpressions {
            case UnresolvedAttribute(Seq(u)) =>
              inputMap.getOrElse(u,
                sys.error(s"Invalid Test: Cannot resolve $u given input $inputMap"))
          }
      }
    )
    resolvedPlan.executeCollectPublic().toSeq
  }
}

