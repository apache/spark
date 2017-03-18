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
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlanner, SparkSqlParser}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.sql.streaming.StreamingQueryManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]] backed by Hive.
 * @param sparkContext The [[SparkContext]].
 * @param sharedState The shared state.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods The experimental methods.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param catalog Internal catalog for managing table and database states that uses Hive client for
 *                interacting with the metastore.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param metadataHive The Hive metadata client.
 * @param analyzer Logical query plan analyzer for resolving unresolved attributes and relations.
 * @param streamingQueryManager Interface to start and stop
 *                              [[org.apache.spark.sql.streaming.StreamingQuery]]s.
 * @param queryExecutionCreator Lambda to create a [[QueryExecution]] from a [[LogicalPlan]]
 * @param plannerCreator Lambda to create a planner that takes into account Hive-specific strategies
 */
private[hive] class HiveSessionState(
    sparkContext: SparkContext,
    sharedState: SharedState,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods,
    functionRegistry: FunctionRegistry,
    override val catalog: HiveSessionCatalog,
    sqlParser: ParserInterface,
    val metadataHive: HiveClient,
    analyzer: Analyzer,
    streamingQueryManager: StreamingQueryManager,
    queryExecutionCreator: LogicalPlan => QueryExecution,
    val plannerCreator: () => SparkPlanner)
  extends SessionState(
      sparkContext,
      sharedState,
      conf,
      experimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      analyzer,
      streamingQueryManager,
      queryExecutionCreator) { self =>

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override def planner: SparkPlanner = plannerCreator()


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

  /**
   * Get an identical copy of the `HiveSessionState`.
   * This should ideally reuse the `SessionState.clone` but cannot do so.
   * Doing that will throw an exception when trying to clone the catalog.
   */
  override def clone(newSparkSession: SparkSession): HiveSessionState = {
    val sparkContext = newSparkSession.sparkContext
    val confCopy = conf.clone()
    val functionRegistryCopy = functionRegistry.clone()
    val experimentalMethodsCopy = experimentalMethods.clone()
    val sqlParser: ParserInterface = new SparkSqlParser(confCopy)
    val catalogCopy = catalog.newSessionCatalogWith(
      newSparkSession,
      confCopy,
      SessionState.newHadoopConf(sparkContext.hadoopConfiguration, confCopy),
      functionRegistryCopy,
      sqlParser)
    val queryExecutionCreator = (plan: LogicalPlan) => new QueryExecution(newSparkSession, plan)

    val hiveClient =
      newSparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .newSession()

    SessionState.mergeSparkConf(confCopy, sparkContext.getConf)

    new HiveSessionState(
      sparkContext,
      newSparkSession.sharedState,
      confCopy,
      experimentalMethodsCopy,
      functionRegistryCopy,
      catalogCopy,
      sqlParser,
      hiveClient,
      HiveSessionState.createAnalyzer(newSparkSession, catalogCopy, confCopy),
      new StreamingQueryManager(newSparkSession),
      queryExecutionCreator,
      HiveSessionState.createPlannerCreator(
        newSparkSession,
        confCopy,
        experimentalMethodsCopy))
  }

}

private[hive] object HiveSessionState {

  def apply(sparkSession: SparkSession): HiveSessionState = {
    apply(sparkSession, new SQLConf)
  }

  def apply(sparkSession: SparkSession, conf: SQLConf): HiveSessionState = {
    val initHelper = SessionState(sparkSession, conf)

    val sparkContext = sparkSession.sparkContext

    val catalog = HiveSessionCatalog(
      sparkSession,
      initHelper.functionRegistry,
      initHelper.conf,
      SessionState.newHadoopConf(sparkContext.hadoopConfiguration, initHelper.conf),
      initHelper.sqlParser)

    val metadataHive: HiveClient =
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .newSession()

    val analyzer: Analyzer = createAnalyzer(sparkSession, catalog, initHelper.conf)

    val plannerCreator = createPlannerCreator(
      sparkSession,
      initHelper.conf,
      initHelper.experimentalMethods)

    val hiveSessionState = new HiveSessionState(
      sparkContext,
      sparkSession.sharedState,
      initHelper.conf,
      initHelper.experimentalMethods,
      initHelper.functionRegistry,
      catalog,
      initHelper.sqlParser,
      metadataHive,
      analyzer,
      initHelper.streamingQueryManager,
      initHelper.queryExecutionCreator,
      plannerCreator)
    catalog.functionResourceLoader = hiveSessionState.functionResourceLoader
    hiveSessionState
  }

  /**
   * Create an logical query plan `Analyzer` with rules specific to a `HiveSessionState`.
   */
  private def createAnalyzer(
      sparkSession: SparkSession,
      catalog: HiveSessionCatalog,
      sqlConf: SQLConf): Analyzer = {
    new Analyzer(catalog, sqlConf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveHiveSerdeTable(sparkSession) ::
        new FindDataSourceTable(sparkSession) ::
        new ResolveSQLOnFile(sparkSession) :: Nil

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        new DetermineTableStats(sparkSession) ::
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        PreprocessTableCreation(sparkSession) ::
        PreprocessTableInsertion(sqlConf) ::
        DataSourceAnalysis(sqlConf) ::
        HiveAnalysis :: Nil

      override val extendedCheckRules = Seq(PreWriteCheck)
    }
  }

  private def createPlannerCreator(
      associatedSparkSession: SparkSession,
      sqlConf: SQLConf,
      experimentalMethods: ExperimentalMethods): () => SparkPlanner = {
    () =>
      new SparkPlanner(
          associatedSparkSession.sparkContext,
          sqlConf,
          experimentalMethods.extraStrategies)
        with HiveStrategies {

        override val sparkSession: SparkSession = associatedSparkSession

        override def strategies: Seq[Strategy] = {
          experimentalMethods.extraStrategies ++ Seq(
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
}
