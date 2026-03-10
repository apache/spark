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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.resolver.{
  ExplicitlyUnsupportedResolverFeature,
  Resolver
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.SharedSparkSession

class ExplicitlyUnsupportedResolverFeatureSuite extends QueryTest with SharedSparkSession {
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

  test("Generator with LCAs") {
    checkResolution(
      "SELECT 1 AS a, a, explode(array(1, 2)) FROM VALUES 1;",
      expectedMessage = Some("Generator expressions with LCAs are not supported")
    )
  }

  test("Referencing generator output in other expressions") {
    checkResolution(
      "SELECT explode(array(1, 2, 3)) AS col, col + 1 AS col2;",
      expectedMessage = Some("Referencing generator output in other expressions")
    )
  }

  test("Referencing generator multi-alias output in other expressions") {
    checkResolution(
      "SELECT posexplode(array('x', 'y')) AS (pos, val), pos + 1 AS pos2;",
      expectedMessage = Some("Referencing generator output in other expressions")
    )
  }

  test("Generator with window expressions") {
    checkResolution(
      "SELECT explode(array(1, 2)), RANK() OVER (ORDER BY col1) FROM VALUES 1;",
      expectedMessage = Some("Generator expressions with window expressions are not supported")
    )
  }

  test("Multiple generators in a single SELECT") {
    checkResolution(
      "SELECT explode(array(1, 2)), explode(array('a', 'b')) FROM VALUES 1;",
      expectedMessage = Some("Multiple generator expressions in a single SELECT are not supported")
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
