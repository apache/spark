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
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SparkPlanner, SparkSqlParser}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{NonClosableMutableURLClassLoader, SessionState, SQLConf}
import org.apache.spark.sql.streaming.StreamingQueryManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]] backed by Hive.
 */
private[hive] class HiveSessionState(
    sparkContext: SparkContext,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods,
    functionRegistry: FunctionRegistry,
    override val catalog: HiveSessionCatalog,
    sqlParser: ParserInterface,
    val metadataHive: HiveClient,
    override val analyzer: Analyzer,
    streamingQueryManager: StreamingQueryManager,
    queryExecutionCreator: LogicalPlan => QueryExecution,
    jarClassLoader: NonClosableMutableURLClassLoader,
    val plannerCreator: () => SparkPlanner)
  extends SessionState(
      sparkContext,
      conf,
      experimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      analyzer,
      streamingQueryManager,
      queryExecutionCreator,
      jarClassLoader) { self =>

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

  override def copy(associatedSparkSession: SparkSession): HiveSessionState = {
    val sqlConf = conf.copy
    val copyHelper = SessionState(associatedSparkSession, Some(sqlConf))
    val catalogCopy = catalog.copy(associatedSparkSession)
    val hiveClient =
      associatedSparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .newSession()

    new HiveSessionState(
      associatedSparkSession.sparkContext,
      sqlConf,
      copyHelper.experimentalMethods,
      copyHelper.functionRegistry,
      catalogCopy,
      copyHelper.sqlParser,
      hiveClient,
      HiveSessionState.createAnalyzer(associatedSparkSession, catalogCopy, sqlConf),
      copyHelper.streamingQueryManager,
      copyHelper.queryExecutionCreator,
      jarClassLoader,
      HiveSessionState.createPlannerCreator(
        associatedSparkSession,
        sqlConf,
        copyHelper.experimentalMethods))
  }

}

object HiveSessionState {

  def apply(sparkSession: SparkSession): HiveSessionState = {
    apply(sparkSession, None)
  }

  def apply(
      associatedSparkSession: SparkSession,
      conf: Option[SQLConf]): HiveSessionState = {

    val sparkContext = associatedSparkSession.sparkContext

    val sqlConf = conf.getOrElse(new SQLConf)

    sparkContext.getConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }

    val functionRegistry = FunctionRegistry.builtin.copy

    val experimentalMethods = new ExperimentalMethods

    val jarClassLoader: NonClosableMutableURLClassLoader =
      associatedSparkSession.sharedState.jarClassLoader

    val functionResourceLoader: FunctionResourceLoader =
      SessionState.createFunctionResourceLoader(sparkContext, jarClassLoader)

    val sqlParser: ParserInterface = new SparkSqlParser(sqlConf)

    val catalog = new HiveSessionCatalog(
      associatedSparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      associatedSparkSession.sharedState.globalTempViewManager,
      associatedSparkSession,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      SessionState.newHadoopConf(sparkContext.hadoopConfiguration, sqlConf),
      sqlParser)

    // A Hive client used for interacting with the metastore.
    val metadataHive: HiveClient =
      associatedSparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .newSession()

    // An analyzer that uses the Hive metastore.
    val analyzer: Analyzer = createAnalyzer(associatedSparkSession, catalog, sqlConf)

    val streamingQueryManager: StreamingQueryManager =
      new StreamingQueryManager(associatedSparkSession)

    val queryExecutionCreator = (plan: LogicalPlan) =>
      new QueryExecution(associatedSparkSession, plan)

    val plannerCreator = createPlannerCreator(associatedSparkSession, sqlConf, experimentalMethods)

    new HiveSessionState(
      sparkContext,
      sqlConf,
      experimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      metadataHive,
      analyzer,
      streamingQueryManager,
      queryExecutionCreator,
      jarClassLoader,
      plannerCreator)
  }

  def createAnalyzer(
      sparkSession: SparkSession,
      catalog: HiveSessionCatalog,
      sqlConf: SQLConf): Analyzer = {

    new Analyzer(catalog, sqlConf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
          catalog.OrcConversions ::
          new DetermineHiveSerde(sqlConf) ::
          new FindDataSourceTable(sparkSession) ::
          new FindHiveSerdeTable(sparkSession) ::
          new ResolveDataSource(sparkSession) :: Nil

      override val postHocResolutionRules =
        AnalyzeCreateTable(sparkSession) ::
          PreprocessTableInsertion(sqlConf) ::
          DataSourceAnalysis(sqlConf) ::
          new HiveAnalysis(sparkSession) :: Nil

      override val extendedCheckRules = Seq(PreWriteCheck(sqlConf, catalog))
    }
  }

  def createPlannerCreator(
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
            DDLStrategy,
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
