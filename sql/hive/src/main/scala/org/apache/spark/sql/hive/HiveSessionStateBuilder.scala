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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.TableCapabilityCheck
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.execution.PruneHiveTablePartitions
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionResourceLoader, SessionState}

/**
 * Builder that produces a Hive-aware `SessionState`.
 */
class HiveSessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState],
    options: Map[String, String])
  extends BaseSessionStateBuilder(session, parentState, options) {

  private def externalCatalog: ExternalCatalogWithListener = session.sharedState.externalCatalog

  /**
   * Create a Hive aware resource loader.
   */
  override protected lazy val resourceLoader: HiveSessionResourceLoader = {
    new HiveSessionResourceLoader(
      session, () => externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client)
  }

  /**
   * Create a [[HiveSessionCatalog]].
   */
  override protected lazy val catalog: HiveSessionCatalog = {
    val catalog = new HiveSessionCatalog(
      () => externalCatalog,
      () => session.sharedState.globalTempViewManager,
      new HiveMetastoreCatalog(session),
      functionRegistry,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  /**
   * A logical query plan `Analyzer` with rules specific to Hive.
   */
  override protected def analyzer: Analyzer = new Analyzer(catalogManager) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new FallBackFileSourceV2(session) +:
        ResolveEncodersInScalaAgg +:
        new ResolveSessionCatalog(
          catalogManager, catalog.isTempView, catalog.isTempFunction) +:
        customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      DetectAmbiguousSelfJoin +:
        new DetermineTableStats(session) +:
        RelationConversions(catalog) +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion +:
        DataSourceAnalysis +:
        HiveAnalysis +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        TableCapabilityCheck +:
        CommandCheck +:
        customCheckRules
  }

  override def customEarlyScanPushDownRules: Seq[Rule[LogicalPlan]] =
    Seq(new PruneHiveTablePartitions(session))

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override protected def planner: SparkPlanner = {
    new SparkPlanner(session, experimentalMethods) with HiveStrategies {
      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies ++
          Seq(HiveTableScans, HiveScripts)
    }
  }

  override protected def newBuilder: NewBuilder = new HiveSessionStateBuilder(_, _, Map.empty)
}

class HiveSessionResourceLoader(
    session: SparkSession,
    clientBuilder: () => HiveClient)
  extends SessionResourceLoader(session) {
  private lazy val client = clientBuilder()
  override def addJar(path: String): Unit = {
    client.addJar(path)
    super.addJar(path)
  }
}
