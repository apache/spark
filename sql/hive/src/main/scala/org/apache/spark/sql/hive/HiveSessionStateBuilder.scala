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

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionResourceLoader, SessionState}

/**
 * Builder that produces a Hive-aware `SessionState`.
 */
@Experimental
@InterfaceStability.Unstable
class HiveSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  /**
   * Create a Hive aware resource loader.
   */
  override protected lazy val resourceLoader: HiveSessionResourceLoader = {
    val client: HiveClient = externalCatalog.client.newSession()
    new HiveSessionResourceLoader(session, client)
  }

  /**
   * Create a [[HiveSessionCatalog]].
   */
  override protected lazy val catalog: HiveSessionCatalog = {
    val catalog = new HiveSessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      new HiveMetastoreCatalog(session),
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  /**
   * A logical query plan `Analyzer` with rules specific to Hive.
   */
  override protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new ResolveHiveSerdeTable(session) +:
      new FindDataSourceTable(session) +:
      new ResolveSQLOnFile(session) +:
      customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      new DetermineTableStats(session) +:
      RelationConversions(conf, catalog) +:
      PreprocessTableCreation(session) +:
      PreprocessTableInsertion(conf) +:
      DataSourceAnalysis(conf) +:
      HiveAnalysis +:
      customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
      customCheckRules
  }

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods) with HiveStrategies {
      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++
          extraPlanningStrategies ++ Seq(
          FileSourceStrategy,
          DataSourceStrategy(conf),
          SpecialLimits,
          InMemoryScans,
          HiveTableScans,
          Scripts,
          Aggregation,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }

  override protected def newBuilder: NewBuilder = new HiveSessionStateBuilder(_, _)
}

class HiveSessionResourceLoader(
    session: SparkSession,
    client: HiveClient)
  extends SessionResourceLoader(session) {
  override def addJar(path: String): Unit = {
    client.addJar(path)
    super.addJar(path)
  }
}
