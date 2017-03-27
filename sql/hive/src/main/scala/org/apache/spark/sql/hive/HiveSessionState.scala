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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlanner}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionFunctionResourceLoader, SessionState, SharedState, SQLConf}
import org.apache.spark.sql.streaming.StreamingQueryManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]] backed by Hive.
 *
 * @param sparkContext The [[SparkContext]].
 * @param sharedState The shared state.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods The experimental methods.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param catalog Internal catalog for managing table and database states that uses Hive client for
 *                interacting with the metastore.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param analyzer Logical query plan analyzer for resolving unresolved attributes and relations.
 * @param optimizer Logical query plan optimizer.
 * @param planner Planner that converts optimized logical plans to physical plans and that takes
 *                Hive-specific strategies into account.
 * @param streamingQueryManager Interface to start and stop streaming queries.
 * @param createQueryExecution Function used to create QueryExecution objects.
 * @param createClone Function used to create clones of the session state.
 * @param metadataHive The Hive metadata client.
 */
private[hive] class HiveSessionState(
    sparkContext: SparkContext,
    sharedState: SharedState,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods,
    functionRegistry: FunctionRegistry,
    override val catalog: HiveSessionCatalog,
    sqlParser: ParserInterface,
    analyzer: Analyzer,
    optimizer: Optimizer,
    planner: SparkPlanner,
    streamingQueryManager: StreamingQueryManager,
    createQueryExecution: LogicalPlan => QueryExecution,
    createClone: (SparkSession, SessionState) => SessionState,
    val metadataHive: HiveClient)
  extends SessionState(
      sparkContext,
      sharedState,
      conf,
      experimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      analyzer,
      optimizer,
      planner,
      streamingQueryManager,
      createQueryExecution,
      createClone) {

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  override def addJar(path: String): Unit = {
    metadataHive.addJar(path)
    super.addJar(path)
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  def convertMetastoreParquet: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET)
  }

  /**
   * When true, also tries to merge possibly different but compatible Parquet schemas in different
   * Parquet data files.
   *
   * This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true.
   */
  def convertMetastoreParquetWithSchemaMerging: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING)
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the Orc SerDe
   * are automatically converted to use the Spark SQL ORC table scan, instead of the Hive
   * SerDe.
   */
  def convertMetastoreOrc: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  /**
   * When true, Hive Thrift server will execute SQL queries asynchronously using a thread pool."
   */
  def hiveThriftServerAsync: Boolean = {
    conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
  }
}

private[hive] object HiveSessionState {
  /**
   * Create a new [[HiveSessionState]] for the given session.
   */
  def apply(session: SparkSession): HiveSessionState = {
    new HiveSessionStateBuilder(session).build()
  }
}

/**
 * Builder that produces a [[HiveSessionState]].
 */
@Experimental
@InterfaceStability.Unstable
class HiveSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

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
      new SessionFunctionResourceLoader(session))
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
      catalog.ParquetConversions +:
      catalog.OrcConversions +:
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
          DataSourceStrategy,
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

  override def build(): HiveSessionState = {
    val metadataHive: HiveClient = externalCatalog.client.newSession()
    new HiveSessionState(
      session.sparkContext,
      session.sharedState,
      conf,
      experimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      analyzer,
      optimizer,
      planner,
      streamingQueryManager,
      createQueryExecution,
      createClone,
      metadataHive)
  }
}
