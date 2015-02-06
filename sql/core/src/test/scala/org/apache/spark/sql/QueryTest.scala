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

import java.util.{Locale, TimeZone}

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.columnar.InMemoryRelation

class QueryTest extends PlanTest {

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

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
   * @param expectedAnswer the expected result, can either be an Any, Seq[Product], or Seq[ Seq[Any] ].
   */
  protected def checkAnswer(rdd: DataFrame, expectedAnswer: Seq[Row]): Unit = {
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
        fail(
          s"""
            |Exception thrown while executing query:
            |${rdd.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      fail(s"""
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
      """.stripMargin)
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

  /**
   * Asserts that a given [[DataFrame]] will be executed using the given number of cached results.
   */
  def assertCached(query: DataFrame, numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

}
