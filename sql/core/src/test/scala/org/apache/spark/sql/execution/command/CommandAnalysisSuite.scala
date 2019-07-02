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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSQLContext

class CommandAnalysisSuite extends PlanTest with SharedSQLContext {
  val parser = new SparkSqlParser(conf)
  val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf)
  val analyzer = new Analyzer(catalog, conf) {
    override def checkAnalysis(plan: LogicalPlan): Unit = {
      val planToCheck = plan match {
        case e: InsertIntoDataSourceDirCommand => e.query
        case _ => plan
      }
      super.checkAnalysis(planToCheck)
    }
  }

  test("SPARK-28195: checkAnalysis should work for InsertIntoDataSourceDirCommand") {
    val query = "insert overwrite directory '/path' using parquet select * from table1"
    val exception = intercept[AnalysisException](
      analyzer.executeAndCheck(parser.parsePlan(query), new QueryPlanningTracker))
    assert(exception.getMessage.contains("Table or view not found: table1"))
  }
}
