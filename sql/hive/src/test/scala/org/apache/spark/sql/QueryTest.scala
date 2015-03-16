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

import scala.collection.JavaConversions._

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
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param rdd the [[DataFrame]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  def checkExistence(rdd: DataFrame, exists: Boolean, keywords: String*) {
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
   * @param rdd the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(rdd: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(rdd, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(rdd: DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(rdd, Seq(expectedAnswer))
  }

  def sqlTest(sqlString: String, expectedAnswer: Seq[Row])(implicit sqlContext: SQLContext): Unit = {
    test(sqlString) {
      checkAnswer(sqlContext.sql(sqlString), expectedAnswer)
    }
  }
}

object QueryTest {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   * @param rdd the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def checkAnswer(rdd: DataFrame, expectedAnswer: Seq[Row]): Option[String] = {
    val isSorted = rdd.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case o => o
        })
      }
      if (!isSorted) converted.sortBy(_.toString) else converted
    }
    val sparkAnswer = try rdd.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
            |Exception thrown while executing query:
            |${rdd.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
        |Results do not match for query:
        |${rdd.logicalPlan}
        |== Analyzed Plan ==
        |${rdd.queryExecution.analyzed}
        |== Physical Plan ==
        |${rdd.queryExecution.executedPlan}
        |== Results ==
        |${sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString)).mkString("\n")}
      """.stripMargin
      return Some(errorMessage)
    }

    return None
  }

  def checkAnswer(rdd: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(rdd, expectedAnswer.toSeq) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }
}
