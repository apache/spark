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

package org.apache.spark.sql

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.{ExprId, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._


/**
 * *** DUPLICATED FROM sql/core. ***
 *
 * It is hard to have maven allow one subproject depend on another subprojects test code.
 * So, we duplicate this code here.
 */
class QueryTest extends PlanTest {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param rdd the [[SchemaRDD]] to be executed
   * @param expectedAnswer the expected result, can either be an Any, Seq[Product], or Seq[ Seq[Any] ].
   */
  protected def checkAnswer(rdd: SchemaRDD, expectedAnswer: Any): Unit = {
    val convertedAnswer = expectedAnswer match {
      case s: Seq[_] if s.isEmpty => s
      case s: Seq[_] if s.head.isInstanceOf[Product] &&
        !s.head.isInstanceOf[Seq[_]] => s.map(_.asInstanceOf[Product].productIterator.toIndexedSeq)
      case s: Seq[_] => s
      case singleItem => Seq(Seq(singleItem))
    }

    val isSorted = rdd.logicalPlan.collect { case s: logical.Sort => s}.nonEmpty
    def prepareAnswer(answer: Seq[Any]) = if (!isSorted) answer.sortBy(_.toString) else answer
    val sparkAnswer = try rdd.collect().toSeq catch {
      case e: Exception =>
        fail(
          s"""
            |Exception thrown while executing query:
            |${rdd.queryExecution}
            |== Exception ==
            |${stackTraceToString(e)}
          """.stripMargin)
    }

    if(prepareAnswer(convertedAnswer) != prepareAnswer(sparkAnswer)) {
      fail(s"""
        |Results do not match for query:
        |${rdd.logicalPlan}
        |== Analyzed Plan ==
        |${rdd.queryExecution.analyzed}
        |== Physical Plan ==
        |${rdd.queryExecution.executedPlan}
        |== Results ==
        |${sideBySide(
            s"== Correct Answer - ${convertedAnswer.size} ==" +:
              prepareAnswer(convertedAnswer).map(_.toString),
            s"== Spark Answer - ${sparkAnswer.size} ==" +:
              prepareAnswer(sparkAnswer).map(_.toString)).mkString("\n")}
      """.stripMargin)
    }
  }
}
