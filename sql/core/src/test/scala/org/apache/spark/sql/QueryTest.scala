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

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._

class QueryTest extends PlanTest {
  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param rdd the [[SchemaRDD]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  def checkExistence(rdd: SchemaRDD, exists: Boolean, keywords: String*) {
    val outputs = rdd.collect().map(_.mkString).mkString
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $rdd ($key doens't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $rdd ($key existed in the result)")
      }
    }
  }

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

    val isSorted = rdd.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    def prepareAnswer(answer: Seq[Any]) = if (!isSorted) answer.sortBy(_.toString) else answer
    val sparkAnswer = try rdd.collect().toSeq catch {
      case e: Exception =>
        fail(
          s"""
            |Exception thrown while executing query:
            |${rdd.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin)
    }

    if (prepareAnswer(convertedAnswer) != prepareAnswer(sparkAnswer)) {
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
