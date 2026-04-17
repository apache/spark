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

package org.apache.spark.sql.test

import org.scalatest.Suite

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util._

/**
 * Helper trait that should be extended by all SQL test suites within the Spark
 * code base. Now a thin alias for [[org.apache.spark.sql.QueryTestBase]] with
 * [[org.apache.spark.sql.catalyst.plans.PlanTest]].
 *
 * Note: This remains a trait (not extending the abstract class QueryTest) so that it can be
 * mixed into classes that extend SparkFunSuite or SparkPlanTest directly.
 */
private[sql] trait SQLTestUtils
  extends org.apache.spark.sql.QueryTestBase
  with org.apache.spark.sql.catalyst.plans.PlanTest

/**
 * Helper trait that can be extended by all external SQL test suites.
 * Now a thin alias for [[org.apache.spark.sql.QueryTestBase]].
 */
private[sql] trait SQLTestUtilsBase
  extends org.apache.spark.sql.QueryTestBase { self: Suite =>
}

private[sql] object SQLTestUtils {

  def compareAnswers(
      sparkAnswer: Seq[Row],
      expectedAnswer: Seq[Row],
      sort: Boolean): Option[String] = {
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      if (sort) {
        converted.sortBy(_.toString())
      } else {
        converted
      }
    }
    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           | == Results ==
           | ${sideBySide(
          s"== Expected Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Actual Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")}
      """.stripMargin
      Some(errorMessage)
    } else {
      None
    }
  }
}
