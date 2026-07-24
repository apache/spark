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

import org.apache.spark.sql.catalyst.analysis.resolver.{
  ExplicitlyUnsupportedResolverFeature,
  Resolver
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.SharedSparkSession

class ExplicitlyUnsupportedResolverFeatureSuite extends SharedSparkSession {
  test("Unsupported table types") {
    withTable("csv_table") {
      spark.sql("CREATE TABLE csv_table (col1 INT) USING CSV;").collect()
      checkResolution("SELECT * FROM csv_table;", shouldPass = true)
    }
    withTable("json_table") {
      spark.sql("CREATE TABLE json_table (col1 INT) USING JSON;").collect()
      checkResolution("SELECT * FROM json_table;", shouldPass = true)
    }
    withTable("parquet_table") {
      spark.sql("CREATE TABLE parquet_table (col1 INT) USING PARQUET;").collect()
      checkResolution("SELECT * FROM parquet_table;", shouldPass = true)
    }
    withTable("orc_table") {
      spark.sql("CREATE TABLE orc_table (col1 INT) USING ORC;").collect()
      checkResolution("SELECT * FROM orc_table;", shouldPass = true)
    }
  }

  test("SPARK-57353: HAVING with grouping analytics is unsupported (SPARK-57346)") {
    checkResolution(
      """SELECT a, SUM(b) FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY ROLLUP(a) HAVING SUM(b) > 30""".stripMargin,
      shouldPass = false,
      expectedMessage = Some("HAVING with grouping analytics (SPARK-57346)")
    )
  }

  test("SPARK-57353: ORDER BY with grouping analytics is unsupported (SPARK-57346)") {
    checkResolution(
      """SELECT a, SUM(b) as s FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY CUBE(a) ORDER BY s""".stripMargin,
      shouldPass = false,
      expectedMessage = Some("ORDER BY with grouping analytics (SPARK-57346)")
    )
  }

  test("SPARK-57353: LCA with grouping analytics is unsupported") {
    checkResolution(
      """SELECT a, SUM(b) as total, total + 1
        |FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY CUBE(a)""".stripMargin,
      shouldPass = false,
      expectedMessage = Some("lateral column alias with grouping analytics")
    )
  }

  private def checkResolution(
      sqlText: String,
      shouldPass: Boolean = false,
      expectedMessage: Option[String] = None): Unit = {
    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(sqlText)
    checkPlanResolution(unresolvedPlan, shouldPass, expectedMessage)
  }

  private def checkPlanResolution(
      plan: LogicalPlan,
      shouldPass: Boolean,
      expectedMessage: Option[String]): Unit = {
    val resolver = new Resolver(
      spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions,
      metadataResolverExtensions = spark.sessionState.analyzer.singlePassMetadataResolverExtensions
    )

    if (shouldPass) {
      resolver.lookupMetadataAndResolve(plan)
    } else {
      val exception = intercept[ExplicitlyUnsupportedResolverFeature] {
        resolver.lookupMetadataAndResolve(plan)
      }
      assert(exception.getMessage.contains(expectedMessage.get))
    }
  }
}
