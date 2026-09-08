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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, ScriptInputOutputSchema, ScriptTransformation}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StringType

class RestrictedModeSuite extends AnalysisTest {

  private val restricted = StaticSQLConf.RESTRICTED_MODE_ENABLED.key
  // The config appears in the error message double-quoted, as `toSQLConf` renders it.
  private val configName = "\"" + restricted + "\""

  // The mode is a static config, so `withSQLConf` (which rejects static keys) cannot toggle it.
  // Set it directly on the active conf instead, restoring the previous value afterwards.
  private def withRestrictedMode[T](enabled: Boolean)(f: => T): T = {
    val conf = SQLConf.get
    val previous = if (conf.contains(restricted)) Some(conf.getConfString(restricted)) else None
    conf.setConfString(restricted, enabled.toString)
    try f finally {
      previous match {
        case Some(value) => conf.setConfString(restricted, value)
        case None => conf.unsetConf(restricted)
      }
    }
  }

  private def functionProject(name: String): LogicalPlan =
    Project(
      Seq(UnresolvedAlias(
        UnresolvedFunction(
          name,
          Seq(Literal("java.lang.Math"), Literal("abs"), Literal(-1)),
          isDistinct = false))),
      TestRelations.testRelation)

  private def transformPlan: ScriptTransformation =
    ScriptTransformation(
      "cat",
      Seq(AttributeReference("value", StringType)()),
      TestRelations.testRelation,
      ScriptInputOutputSchema(Nil, Nil, None, None, Nil, Nil, None, None, schemaLess = false))

  private def checkRestrictedError(e: AnalysisException, feature: String): Unit = {
    checkError(
      exception = e,
      condition = "UNSUPPORTED_FEATURE.SQL_RESTRICTED_MODE",
      parameters = Map("feature" -> feature, "config" -> configName))
  }

  test("reflect / java_method / try_reflect are rejected only when restricted mode is enabled") {
    Seq("reflect", "java_method", "try_reflect").foreach { fn =>
      withRestrictedMode(true) {
        val analyzer = getAnalyzer
        val e = intercept[AnalysisException] {
          analyzer.checkAnalysis(analyzer.execute(functionProject(fn)))
        }
        checkRestrictedError(e, s"The `$fn` function")
      }
      withRestrictedMode(false) {
        val analyzer = getAnalyzer
        analyzer.checkAnalysis(analyzer.execute(functionProject(fn)))
      }
    }
  }

  test("TRANSFORM ... USING is rejected only when restricted mode is enabled") {
    withRestrictedMode(true) {
      val analyzer = getAnalyzer
      val e = intercept[AnalysisException] {
        analyzer.checkAnalysis(analyzer.execute(transformPlan))
      }
      checkRestrictedError(e, "The TRANSFORM ... USING clause")
    }
    withRestrictedMode(false) {
      val analyzer = getAnalyzer
      // Analysis succeeds (no restricted-mode error) when the profile is off.
      analyzer.checkAnalysis(analyzer.execute(transformPlan))
    }
  }

  test("restricted mode is enforced for an analyzed sub-plan under a fresh parent") {
    // An analyzed sub-plan reused under a new query (as a temporary view or a cached Dataset
    // stores it): `setAnalyzed()` marks the sub-tree analyzed, so `checkAnalysis0`'s
    // `case p if p.analyzed` skips it. A fresh, un-analyzed parent reproduces that shape;
    // `checkRestrictedMode` must still reach the feature inside the analyzed child.
    val analyzedChild = withRestrictedMode(false) {
      val plan = getAnalyzer.execute(transformPlan)
      plan.setAnalyzed()
      plan
    }
    val parent = Project(analyzedChild.output, analyzedChild)
    withRestrictedMode(true) {
      val e = intercept[AnalysisException](getAnalyzer.checkAnalysis(parent))
      checkRestrictedError(e, "The TRANSFORM ... USING clause")
    }
  }
}
