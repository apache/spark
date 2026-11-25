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
package org.apache.spark.sql.hive

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.UI
import org.apache.spark.sql.{SparkSession => SqlSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.util.Utils

/**
 * HiveSessionStateBuilder replaces Analyzer and must copy extension hook lists
 * from BaseSessionStateBuilder. Without hintResolutionRules, injectHintResolutionRule
 * is a no-op under enableHiveSupport() (Hive Thrift / jdbc:hive2).
 *
 * SPARK-59081: Do not use TestHiveSingleton: that session is already built, so
 * withExtensions on a new builder is required.
 */
class HiveSparkSessionExtensionSuite extends SparkFunSuite {

  // Isolated sessions leave a SparkContext running; do not treat that as a leak.
  override protected val enableAutoThreadAudit = false

  private def withHiveSession(
      builders: Seq[SparkSessionExtensions => Unit])(f: SparkSession => Unit): Unit = {
    val savedActive = SparkSession.getActiveSession
    val savedDefault = SparkSession.getDefaultSession
    val builder = SparkSession.builder()
      .master("local[1]")
      .config(UI.UI_ENABLED.key, false)
      .enableHiveSupport()
    HiveUtils.newTemporaryConfiguration(useInMemoryDerby = true).foreach {
      case (k, v) => builder.config(k, v)
    }
    builders.foreach(builder.withExtensions)
    val session = try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      builder.create()
    } finally {
      savedDefault.foreach(SparkSession.setDefaultSession)
      savedActive.foreach(SparkSession.setActiveSession)
    }
    try {
      f(session)
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      savedDefault.foreach(SparkSession.setDefaultSession)
      savedActive.foreach(SparkSession.setActiveSession)
    }
  }

  case class MyHintRule(spark: SqlSession) extends Rule[LogicalPlan] {
    val myHintName = Set("CONVERT_TO_EMPTY")

    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
        case h: UnresolvedHint if myHintName.contains(h.name.toUpperCase(Locale.ROOT)) =>
          LocalRelation(h.output, data = Seq.empty, isStreaming = h.isStreaming)
      }
  }

  test("SPARK-59081: inject custom hint rule") {
    withHiveSession(Seq(_.injectHintResolutionRule(MyHintRule))) { session =>
      assert(session.sessionState.catalog.getClass.getName.contains("HiveSessionCatalog"))
      assert(session.sessionState.analyzer.hintResolutionRules.contains(MyHintRule(session)))
      assert(
        session.range(1).hint("CONVERT_TO_EMPTY").logicalPlan.isInstanceOf[LocalRelation],
        "plan is expected to be a local relation")
    }
  }

  test("SPARK-59081: hint rule sees UnresolvedRelation for path SQL before ResolveSQLOnFile") {
    var hintSawUnresolved = false
    var resolutionSawUnresolved = false

    case class PathSqlHintRule(spark: SqlSession) extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        plan.foreach {
          case r: UnresolvedRelation
              if r.multipartIdentifier.size == 2 &&
                r.multipartIdentifier.head.equalsIgnoreCase("parquet") =>
            hintSawUnresolved = true
          case _ =>
        }
        plan
      }
    }

    case class PathSqlResolutionRule(spark: SqlSession) extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
        case r: UnresolvedRelation
            if r.multipartIdentifier.size == 2 &&
              r.multipartIdentifier.head.equalsIgnoreCase("parquet") =>
          resolutionSawUnresolved = true
          r
      }
    }

    withHiveSession(Seq(
        _.injectHintResolutionRule(PathSqlHintRule),
        _.injectResolutionRule(PathSqlResolutionRule))) { session =>
      val dir = Utils.createTempDir()
      try {
        val path = new java.io.File(dir, "data").getCanonicalPath
        session.range(1).write.parquet(path)
        val df = session.sql(s"SELECT * FROM parquet.`$path`")
        df.queryExecution.analyzed
        assert(hintSawUnresolved,
          "injectHintResolutionRule must run before ResolveSQLOnFile")
        assert(!resolutionSawUnresolved,
          "injectResolutionRule is after ResolveSQLOnFile and must not see the path relation")
      } finally {
        Utils.deleteRecursively(dir)
      }
    }
  }
}
