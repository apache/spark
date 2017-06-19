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
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

trait AnalysisTest extends PlanTest {

  protected val caseSensitiveAnalyzer = makeAnalyzer(caseSensitive = true)
  protected val caseInsensitiveAnalyzer = makeAnalyzer(caseSensitive = false)

  private def makeAnalyzer(caseSensitive: Boolean): Analyzer = {
    val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive)
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    catalog.createTempView("TaBlE", TestRelations.testRelation, overrideIfExists = true)
    catalog.createTempView("TaBlE2", TestRelations.testRelation2, overrideIfExists = true)
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules = EliminateSubqueryAliases :: Nil
    }
  }

  protected def getAnalyzer(caseSensitive: Boolean) = {
    if (caseSensitive) caseSensitiveAnalyzer else caseInsensitiveAnalyzer
  }

  protected def checkAnalysis(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    val analyzer = getAnalyzer(caseSensitive)
    val actualPlan = analyzer.execute(inputPlan)
    analyzer.checkAnalysis(actualPlan)
    comparePlans(actualPlan, expectedPlan)
  }

  protected def assertAnalysisSuccess(
      inputPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    val analyzer = getAnalyzer(caseSensitive)
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

  protected def assertAnalysisError(
      inputPlan: LogicalPlan,
      expectedErrors: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    val analyzer = getAnalyzer(caseSensitive)
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
