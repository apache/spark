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

package org.apache.spark.sql.internal

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.streaming.{StreamingQueryManager}
import org.apache.spark.sql.util.ExecutionListenerManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]].
 */
private[sql] class SessionState(
    sparkSession: SparkSession,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry,
    val catalog: SessionCatalog,
    val sqlParser: ParserInterface) {

  /*
   * Interface exposed to the user for registering user-defined functions.
   * Note that the user-defined functions must be deterministic.
   */
  val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  // Logical query plan analyzer for resolving unresolved attributes and relations.
  val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        new FindDataSourceTable(sparkSession) ::
        new ResolveDataSource(sparkSession) :: Nil

      override val postHocResolutionRules =
        AnalyzeCreateTable(sparkSession) ::
        PreprocessTableInsertion(conf) ::
        DataSourceAnalysis(conf) :: Nil

      override val extendedCheckRules =
        Seq(PreWriteCheck(conf, catalog), HiveOnlyCheck)
    }
  }

  // Logical query plan optimizer.
  val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods)

  /*
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  // Interface to start and stop [[StreamingQuery]]s.
  val streamingQueryManager: StreamingQueryManager = new StreamingQueryManager(sparkSession)

  // Automatically extract all entries and put it in our SQLConf
  // We need to call it after all of vals have been initialized.
  sparkSession.sparkContext.getConf.getAll.foreach { case (k, v) =>
    conf.setConfString(k, v)
  }

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  def planner: SparkPlanner =
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(sparkSession, conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def copy(associatedSparkSession: SparkSession): SessionState = {
    val sqlConf = conf.copy
    val sqlParser: ParserInterface = new SparkSqlParser(sqlConf)

    new SessionState(
      sparkSession,
      sqlConf,
      experimentalMethods.copy,
      functionRegistry.copy,
      catalog.copy,
      sqlParser)
  }

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = new QueryExecution(sparkSession, plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }

  private val jarClassLoader: NonClosableMutableURLClassLoader =
    sparkSession.sharedState.jarClassLoader

  def addJar(path: String): Unit = SessionState.addJar(sparkSession, path, jarClassLoader)

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   */
  def analyze(tableIdent: TableIdentifier, noscan: Boolean = true): Unit = {
    AnalyzeTableCommand(tableIdent, noscan).run(sparkSession)
  }

}


object SessionState {

  def apply(sparkSession: SparkSession): SessionState = {
    apply(sparkSession, None)
  }

  def apply(
      sparkSession: SparkSession,
      conf: Option[SQLConf]): SessionState = {

    // SQL-specific key-value configurations.
    val sqlConf = conf.getOrElse(new SQLConf)

    // Internal catalog for managing functions registered by the user.
    val functionRegistry = FunctionRegistry.builtin.copy

    val jarClassLoader: NonClosableMutableURLClassLoader = sparkSession.sharedState.jarClassLoader

    // A class for loading resources specified by a function.
    val functionResourceLoader: FunctionResourceLoader =
      createFunctionResourceLoader(sparkSession, jarClassLoader)

    // Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
    val sqlParser: ParserInterface = new SparkSqlParser(sqlConf)

    // Internal catalog for managing table and database states.
    val catalog = new SessionCatalog(
      sparkSession.sharedState.externalCatalog,
      sparkSession.sharedState.globalTempViewManager,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      newHadoopConf(sparkSession, sqlConf),
      sqlParser)

    new SessionState(
      sparkSession,
      sqlConf,
      new ExperimentalMethods,
      functionRegistry,
      catalog,
      sqlParser)
  }

  def createFunctionResourceLoader(
      sparkSession: SparkSession,
      jarClassLoader: NonClosableMutableURLClassLoader): FunctionResourceLoader = {

    new FunctionResourceLoader {
      override def loadResource(resource: FunctionResource): Unit = {
        resource.resourceType match {
          case JarResource => addJar(sparkSession, resource.uri, jarClassLoader)
          case FileResource => sparkSession.sparkContext.addFile(resource.uri)
          case ArchiveResource =>
            throw new AnalysisException(
              "Archive is not allowed to be loaded. If YARN mode is used, " +
                "please use --archives options while calling spark-submit.")
        }
      }
    }
  }

  def addJar(
      sparkSession: SparkSession,
      path: String,
      jarClassLoader: NonClosableMutableURLClassLoader): Unit = {

    sparkSession.sparkContext.addJar(path)

    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    jarClassLoader.addURL(jarURL)
    Thread.currentThread().setContextClassLoader(jarClassLoader)
  }

  def newHadoopConf(sparkSession: SparkSession, conf: SQLConf): Configuration = {
    val hadoopConf = new Configuration(sparkSession.sparkContext.hadoopConfiguration)
    conf.getAllConfs.foreach { case (k, v) => if (v ne null) hadoopConf.set(k, v) }
    hadoopConf
  }

}
