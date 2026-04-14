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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{QueryTest, QueryTestBase, Row}
import org.apache.spark.sql.catalyst.plans.PlanTest

/**
 * Helper trait that can be extended by all external SQL test suites.
 *
 * This is now an alias for [[QueryTestBase]].
 */
private[sql] trait SQLTestUtilsBase extends QueryTestBase { self: Suite => }

/**
 * Helper trait that should be extended by all SQL test suites within the Spark
 * code base.
 *
 * This is now an alias for [[QueryTest]].
 */
private[sql] trait SQLTestUtils extends SparkFunSuite with SQLTestUtilsBase with PlanTest

private[sql] object SQLTestUtils {

  def compareAnswers(
      sparkAnswer: Seq[Row],
      expectedAnswer: Seq[Row],
      sort: Boolean): Option[String] = {
    QueryTest.compareAnswers(sparkAnswer, expectedAnswer, sort)
  }
}
