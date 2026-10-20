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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.analysis.resolver.{
  DropTableResolver,
  ResolutionValidator,
  Resolver,
  ResolverRunner
}
import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DropTableResolverSuite extends QueryTest with SharedSparkSession {
  for {
    useV1Command <- Seq(true, false)
    statement <- Seq(
      "DROP TABLE t",
      "DROP TABLE IF EXISTS t",
      "DROP TABLE t PURGE",
      "DROP TABLE default.t",
      "DROP TABLE spark_catalog.default.t",
      "DROP TABLE testcat.ns.t",
      "DROP TABLE IDENTIFIER('t')")
  } {
    test(s"$statement: useV1Command=$useV1Command") {
      withSQLConf(
        SQLConf.LEGACY_USE_V1_COMMAND.key -> useV1Command.toString,
        "spark.sql.catalog.testcat" ->
          "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog") {
        checkEquivalentAnalysis(statement)
      }
    }
  }

  test("DROP TABLE resolves local and global temporary views first") {
    withTempView("drop_table_view") {
      spark.range(1).createOrReplaceTempView("drop_table_view")
      checkEquivalentAnalysis("DROP TABLE drop_table_view")
    }
    withGlobalTempView("drop_table_global_view") {
      spark.range(1).createOrReplaceGlobalTempView("drop_table_global_view")
      checkEquivalentAnalysis("DROP TABLE global_temp.drop_table_global_view")
    }
  }

  test("DROP TABLE preserves identifier tags and origin") {
    val parsed = spark.sessionState.sqlParser.parsePlan("DROP TABLE testcat.ns.t")
      .asInstanceOf[DropTable]
    val tag = TreeNodeTag[String]("drop_table_target")
    parsed.child.setTagValue(tag, "preserved")
    withSQLConf("spark.sql.catalog.testcat" ->
        "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog") {
      val resolved = resolve(parsed).asInstanceOf[DropTable]
      assert(resolved.child.getTagValue(tag).contains("preserved"))
      assert(resolved.child.origin == parsed.child.origin)
    }
  }

  test("DROP TABLE retains V1 namespace validation with either command setting") {
    Seq(true, false).foreach { useV1Command =>
      withSQLConf(SQLConf.LEGACY_USE_V1_COMMAND.key -> useV1Command.toString) {
        val parsed = spark.sessionState.sqlParser.parsePlan("DROP TABLE spark_catalog.a.b.t")
        val fixedPointError = intercept[AnalysisException] {
          spark.sessionState.analyzer.execute(parsed)
        }
        val singlePassError = intercept[AnalysisException] {
          resolve(parsed)
        }
        assert(singlePassError.getCondition == fixedPointError.getCondition)
        assert(singlePassError.getMessageParameters == fixedPointError.getMessageParameters)
      }
    }
  }

  test("DROP TABLE executes through single-pass analysis and the catalog API") {
    withTempView("drop_table_view") {
      spark.range(1).createOrReplaceTempView("drop_table_view")
      withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED.key -> "true",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        spark.catalog.dropTable("drop_table_view")
        sql("DROP TABLE IF EXISTS missing_drop_table_target")
      }
      assert(!spark.catalog.tableExists("drop_table_view"))
    }
  }

  test("validator extensions do not accept unresolved DROP TABLE targets") {
    val validator = new ResolutionValidator(Seq(new DropTableResolver(spark)))
    intercept[AnalysisException] {
      validator.validatePlan(DropTable(UnresolvedIdentifier(Seq("t")), false, false))
    }
  }

  private def checkEquivalentAnalysis(statement: String): Unit = {
    val fixedPoint = spark.sessionState.analyzer.execute(
      spark.sessionState.sqlParser.parsePlan(statement))
    val singlePass = resolve(spark.sessionState.sqlParser.parsePlan(statement))
    comparePlans(singlePass, fixedPoint)
    assert(singlePass.getTagValue(ResolverRunner.SINGLE_PASS_ANALYSIS_MARKER).contains(true))
  }

  private def resolve(plan: LogicalPlan): LogicalPlan = {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED.key -> "true") {
      val resolver = new Resolver(
        catalogManager = spark.sessionState.catalogManager,
        extensions = spark.sessionState.analyzer.singlePassResolverExtensions)
      new ResolverRunner(resolver).resolve(plan, None, new QueryPlanningTracker)
    }
  }
}
