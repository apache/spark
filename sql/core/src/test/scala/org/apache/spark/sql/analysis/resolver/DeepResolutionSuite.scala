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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DeepResolutionSuite extends QueryTest with SharedSparkSession {
  protected override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key, "true")
      .set(SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key, "false")

  test("Unions") {
    spark.sql(generateUnions(1500, selectClause = generateSelectFromValues(50)))
  }

  test("Selects from selects") {
    spark.sql(generateSelectsFromSelects(500, sourceSelectClause = generateSelectFromValues(50)))
  }

  test("Binary arithmetic") {
    spark.sql(
      generateSelectFromValues(50, projection = s"col1 + ${generateAdditions(3000)} + col2")
    )
  }

  private def generateUnions(numberOfUnions: Int, selectClause: String) = {
    (0 until numberOfUnions).map(_ => selectClause).mkString(" UNION ALL ")
  }

  private def generateSelectsFromSelects(numberOfSelects: Int, sourceSelectClause: String) = {
    (0 until numberOfSelects)
      .map(_ => "SELECT * FROM (")
      .mkString(" ") + sourceSelectClause + (0 until numberOfSelects)
      .map(_ => ")")
      .mkString(" ")
  }

  private def generateSelectFromValues(numberOfValues: Int, projection: String = "*") = {
    s"SELECT ${projection} FROM VALUES (${(0 until numberOfValues).mkString(", ")})"
  }

  private def generateAdditions(numberOfAdditions: Int) = {
    (0 until numberOfAdditions).mkString(" + ")
  }
}
