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

import java.util.TimeZone

import org.scalatest.Assertions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.util.SparkErrorUtils

/**
 * Provides [[checkAnswer]] helper for SQL- & DataFrame-API tests.
 *
 * TODO: should be moved to sql/api together with SessionQueryTestBase
 */
@Experimental
trait CheckAnswerHelper extends Assertions {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {

    val analyzedDF = try df catch {
      case ae: ExtendedAnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${SparkErrorUtils.stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    getErrorMessageInCheckAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /*
   * Note: when moving this to sql/api, implementation should stay in sql/core
   * (i.e. only have abstract decl in sql/api)
   */
  protected def isDfSorted(df: DataFrame): Boolean = {
    df match {
      case df: classic.DataFrame =>
        df.logicalPlan.collectFirst { case s: logical.Sort => s }.nonEmpty
      case _ =>
        // isDfSorted should be overridden by connect so that this case can't be reached.
        throw new RuntimeException(
          s"""Cannot determine whether df is sorted: $df.
             |Maybe the suite is missing the connect.SessionQueryTest mixin?""".stripMargin)
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a None will
   * be returned.
   *
   * @param df the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   */
  private def getErrorMessageInCheckAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row]): Option[String] = {
    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${if (df.isInstanceOf[classic.DataFrame]) { df.queryExecution } else df.toString}
             |== Exception ==
             |$e
             |${SparkErrorUtils.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    RowComparisonUtils.sameRows(expectedAnswer, sparkAnswer, isDfSorted(df)).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${if (df.isInstanceOf[classic.DataFrame]) { df.queryExecution } else df.toString }
         |== Results ==
         |$results
       """.stripMargin
    }
  }

}
