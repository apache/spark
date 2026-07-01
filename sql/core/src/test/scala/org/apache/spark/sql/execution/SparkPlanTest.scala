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

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SQLContext}

/**
 * Kept as an empty alias of [[QueryTest]] for backward compatibility with existing subclasses.
 * New test suites should extend [[QueryTest]] directly.
 */
@deprecated("Use QueryTest directly instead.", "4.2.0")
private[sql] trait SparkPlanTest extends QueryTest

@deprecated("Use QueryTest directly instead.", "4.2.0")
private[sql] object SparkPlanTest {

  /**
   * Kept as a thin alias of [[QueryTest.checkAnswer]] for backward compatibility.
   * New callers should use [[QueryTest.checkAnswer]] directly.
   */
  def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedPlanFunction: SparkPlan => SparkPlan,
      sortAnswers: Boolean,
      spark: SQLContext): Option[String] =
    QueryTest.checkAnswer(input, planFunction, expectedPlanFunction, sortAnswers, spark)

  /**
   * Kept as a thin alias of [[QueryTest.checkAnswer]] for backward compatibility.
   * New callers should use [[QueryTest.checkAnswer]] directly.
   */
  def checkAnswer(
      input: Seq[DataFrame],
      planFunction: Seq[SparkPlan] => SparkPlan,
      expectedAnswer: Seq[Row],
      sortAnswers: Boolean,
      spark: SQLContext): Option[String] =
    QueryTest.checkAnswer(input, planFunction, expectedAnswer, sortAnswers, spark)

  /**
   * Kept as a thin alias of [[QueryTest.executePlan]] for backward compatibility.
   * New callers should use [[QueryTest.executePlan]] directly.
   */
  def executePlan(outputPlan: SparkPlan, spark: SQLContext): Seq[Row] =
    QueryTest.executePlan(outputPlan, spark)
}
