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

  test("Unsupported char type padding") {
    withTable("char_type_padding") {
      spark.sql(s"CREATE TABLE t1 (c1 CHAR(3), c2 STRING) USING PARQUET")
      checkResolution("SELECT c1 = '12', c1 = '12 ', c1 = '12  ' FROM t1 WHERE c2 = '12'")
    }
  }

  test("Unsupported star expansion") {
    checkResolution("SELECT * FROM VALUES (1, 2) WHERE 3 IN (*)")
  }

  test("LateralColumnAlias in Aggregate") {
    checkResolution("SELECT 1 AS a, sum(col1) as sum1, a + sum(col2) FROM VALUES(1, 2)")
  }

  test("Unsupported lambda") {
    checkResolution(
      "SELECT array_sort(array(2, 1), (p1, p2) -> CASE WHEN p1 > p2 THEN 1 ELSE 0 END)"
    )
  }

  private def checkResolution(sqlText: String, shouldPass: Boolean = false): Unit = {
    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(sqlText)
    checkResolution(unresolvedPlan, shouldPass)
  }

  private def checkResolution(plan: LogicalPlan, shouldPass: Boolean): Unit = {
    def noopWrapper(body: => Unit) = body

    val wrapper = if (shouldPass) {
      noopWrapper _
    } else {
      intercept[ExplicitlyUnsupportedResolverFeature] _
    }

    val resolver = new Resolver(
      spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions,
      metadataResolverExtensions = spark.sessionState.analyzer.singlePassMetadataResolverExtensions
    )
    wrapper {
      resolver.lookupMetadataAndResolve(plan)
    }
  }
}
