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
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.{SparkPlanner, SparkSqlParser}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{NonClosableMutableURLClassLoader, SessionState, SQLConf}


/**
 * A class that holds all session-specific state in a given [[SparkSession]] backed by Hive.
 */
private[hive] class HiveSessionState(
    sparkSession: SparkSession,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods,
    functionRegistry: FunctionRegistry,
    override val catalog: HiveSessionCatalog,
    sqlParser: ParserInterface,
    val metadataHive: HiveClient)
  extends SessionState(
    sparkSession,
    conf,
    experimentalMethods,
    functionRegistry,
    catalog,
    sqlParser) {

  self =>

  /**
   * An analyzer that uses the Hive metastore.
   */
  override val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        new DetermineHiveSerde(conf) ::
        new FindDataSourceTable(sparkSession) ::
        new FindHiveSerdeTable(sparkSession) ::
        new ResolveDataSource(sparkSession) :: Nil

      override val postHocResolutionRules =
        AnalyzeCreateTable(sparkSession) ::
        PreprocessTableInsertion(conf) ::
        DataSourceAnalysis(conf) ::
        new HiveAnalysis(sparkSession) :: Nil

      override val extendedCheckRules = Seq(PreWriteCheck(conf, catalog))
    }
  }

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override def planner: SparkPlanner = {
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)
      with HiveStrategies {
      override val sparkSession: SparkSession = self.sparkSession

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
    val sqlParser: ParserInterface = new SparkSqlParser(conf)

    new HiveSessionState(
      associatedSparkSession,
      conf.copy,
      experimentalMethods.copy,
      functionRegistry.copy,
      catalog.copy(associatedSparkSession),
      sqlParser,
      metadataHive)
  }

}

object HiveSessionState {

  def apply(sparkSession: SparkSession): HiveSessionState = {
    apply(sparkSession, None)
  }

  def apply(
     sparkSession: SparkSession,
     conf: Option[SQLConf]): HiveSessionState = {

    val sqlConf = conf.getOrElse(new SQLConf)

    val functionRegistry = FunctionRegistry.builtin.copy

    val jarClassLoader: NonClosableMutableURLClassLoader = sparkSession.sharedState.jarClassLoader

    val functionResourceLoader: FunctionResourceLoader =
      SessionState.createFunctionResourceLoader(sparkSession, jarClassLoader)

    val sqlParser: ParserInterface = new SparkSqlParser(sqlConf)

    val catalog = new HiveSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      SessionState.newHadoopConf(sparkSession, sqlConf),
      sqlParser)


    // A Hive client used for interacting with the metastore.
    val metadataHive: HiveClient =
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .newSession()

    new HiveSessionState(
      sparkSession,
      sqlConf,
      new ExperimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      metadataHive)
  }

}
