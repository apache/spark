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

package org.apache.spark.sql.catalyst.analysis

import java.net.URI
import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog, TemporaryViewRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_STORING_ANALYZED_PLAN
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StructType

trait AnalysisTest extends PlanTest {

  protected def extendedAnalysisRules: Seq[Rule[LogicalPlan]] = Nil

  protected def createTempView(
      catalog: SessionCatalog,
      name: String,
      plan: LogicalPlan,
      overrideIfExists: Boolean): Unit = {
    val identifier = TableIdentifier(name)
    val metadata = createTempViewMetadata(identifier, plan.schema)
    val viewDefinition = TemporaryViewRelation(metadata, Some(plan))
    catalog.createTempView(name, viewDefinition, overrideIfExists)
  }

  protected def createGlobalTempView(
      catalog: SessionCatalog,
      name: String,
      plan: LogicalPlan,
      overrideIfExists: Boolean): Unit = {
    val globalDb = Some(SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE))
    val identifier = TableIdentifier(name, globalDb)
    val metadata = createTempViewMetadata(identifier, plan.schema)
    val viewDefinition = TemporaryViewRelation(metadata, Some(plan))
    catalog.createGlobalTempView(name, viewDefinition, overrideIfExists)
  }

  private def createTempViewMetadata(
      identifier: TableIdentifier,
      schema: StructType): CatalogTable = {
    CatalogTable(
      identifier = identifier,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = schema,
      properties = Map((VIEW_STORING_ANALYZED_PLAN, "true")))
  }

  protected def getAnalyzer: Analyzer = {
    val catalog = new SessionCatalog(
      new InMemoryCatalog, FunctionRegistry.builtin, TableFunctionRegistry.builtin)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    createTempView(catalog, "TaBlE", TestRelations.testRelation, overrideIfExists = true)
    createTempView(catalog, "TaBlE2", TestRelations.testRelation2, overrideIfExists = true)
    createTempView(catalog, "TaBlE3", TestRelations.testRelation3, overrideIfExists = true)
    createGlobalTempView(catalog, "TaBlE4", TestRelations.testRelation4, overrideIfExists = true)
    createGlobalTempView(catalog, "TaBlE5", TestRelations.testRelation5, overrideIfExists = true)
    new Analyzer(catalog) {
      override val extendedResolutionRules = extendedAnalysisRules
    }
  }

  protected def checkAnalysis(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
      val analyzer = getAnalyzer
      val actualPlan = analyzer.executeAndCheck(inputPlan, new QueryPlanningTracker)
      comparePlans(EliminateSubqueryAliases(actualPlan), expectedPlan)
    }
  }

  protected def checkAnalysisWithoutViewWrapper(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true,
      inlineCTE: Boolean = false): Unit = {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
      val analyzed = getAnalyzer.executeAndCheck(inputPlan, new QueryPlanningTracker)
      val transformed = EliminateSubqueryAliases(analyzed) transformUp {
        case v: View if v.isTempViewStoringAnalyzedPlan => v.child
      }
      val actualPlan = if (inlineCTE) {
        val inlineCTE = InlineCTE()
        inlineCTE(transformed)
      } else {
        transformed
      }
      comparePlans(actualPlan, expectedPlan)
    }
  }

  protected override def comparePlans(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      checkAnalysis: Boolean = false): Unit = {
    // Analysis tests may have not been fully resolved, so skip checkAnalysis.
    super.comparePlans(plan1, plan2, checkAnalysis)
  }

  protected def assertAnalysisSuccess(
      inputPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
      val analyzer = getAnalyzer
      val analysisAttempt = analyzer.execute(inputPlan)
      try analyzer.checkAnalysis(analysisAttempt) catch {
        case a: AnalysisException =>
          fail(
            s"""
              |Failed to Analyze Plan
              |$inputPlan
              |
              |Partial Analysis
              |$analysisAttempt
            """.stripMargin, a)
      }
    }
  }

  protected def assertAnalysisError(
      inputPlan: LogicalPlan,
      expectedErrors: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
      val analyzer = getAnalyzer
      val e = intercept[AnalysisException] {
        analyzer.checkAnalysis(analyzer.execute(inputPlan))
      }

      if (!expectedErrors.map(_.toLowerCase(Locale.ROOT)).forall(
          e.getMessage.toLowerCase(Locale.ROOT).contains)) {
        fail(
          s"""Exception message should contain the following substrings:
             |
             |  ${expectedErrors.mkString("\n  ")}
             |
             |Actual exception message:
             |
             |  ${e.getMessage}
           """.stripMargin)
      }
    }
  }

  protected def assertAnalysisErrorClass(
      inputPlan: LogicalPlan,
      expectedErrorClass: String,
      expectedMessageParameters: Map[String, String],
      caseSensitive: Boolean): Unit = {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
      val analyzer = getAnalyzer
      val e = intercept[AnalysisException] {
        analyzer.checkAnalysis(analyzer.execute(inputPlan))
      }

      if (e.getErrorClass != expectedErrorClass ||
        !e.messageParameters.sameElements(expectedMessageParameters)) {
        var failMsg = ""
        if (e.getErrorClass != expectedErrorClass) {
          failMsg +=
            s"""Error class should be: ${expectedErrorClass}
               |Actual error class: ${e.getErrorClass}
             """.stripMargin
        }
        if (!e.messageParameters.sameElements(expectedMessageParameters)) {
          failMsg +=
            s"""Message parameters should be: ${expectedMessageParameters.mkString("\n  ")}
               |Actual message parameters: ${e.messageParameters.mkString("\n  ")}
             """.stripMargin
        }
        fail(failMsg)
      }
    }
  }

  protected def interceptParseException(parser: String => Any)(
    sqlCommand: String, messages: String*)(
    errorClass: Option[String] = None): Unit = {
    val e = parseException(parser)(sqlCommand)
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
    if (errorClass.isDefined) {
      assert(e.getErrorClass == errorClass.get)
    }
  }

  protected def parseException(parser: String => Any)(
    sqlText: String): ParseException = {
    intercept[ParseException](parser(sqlText))
  }
}
