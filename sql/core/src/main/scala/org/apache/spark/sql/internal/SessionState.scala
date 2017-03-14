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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.ExecutionListenerManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]].
 * @param sparkContext The [[SparkContext]].
 * @param sharedState The shared state.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods The experimental methods.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param catalog Internal catalog for managing table and database states.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param analyzer Logical query plan analyzer for resolving unresolved attributes and relations.
 * @param streamingQueryManager Interface to start and stop
 *                              [[org.apache.spark.sql.streaming.StreamingQuery]]s.
 * @param queryExecutionCreator Lambda to create a [[QueryExecution]] from a [[LogicalPlan]]
 */
private[sql] class SessionState(
    sparkContext: SparkContext,
    sharedState: SharedState,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry,
    val catalog: SessionCatalog,
    val sqlParser: ParserInterface,
    val analyzer: Analyzer,
    val streamingQueryManager: StreamingQueryManager,
    val queryExecutionCreator: LogicalPlan => QueryExecution) {

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(
    sparkContext.hadoopConfiguration,
    conf)

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
   * A class for loading resources specified by a function.
   */
  val functionResourceLoader: FunctionResourceLoader = {
    new FunctionResourceLoader {
      override def loadResource(resource: FunctionResource): Unit = {
        resource.resourceType match {
          case JarResource => addJar(resource.uri)
          case FileResource => sparkContext.addFile(resource.uri)
          case ArchiveResource =>
            throw new AnalysisException(
              "Archive is not allowed to be loaded. If YARN mode is used, " +
                "please use --archives options while calling spark-submit.")
        }
      }
    }
  }

  /**
   * Interface exposed to the user for registering user-defined functions.
   * Note that the user-defined functions must be deterministic.
   */
  val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  /**
   * Logical query plan optimizer.
   */
  val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods)

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  def planner: SparkPlanner =
    new SparkPlanner(sparkContext, conf, experimentalMethods.extraStrategies)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def clone(newSparkSession: SparkSession): SessionState = {
    val sparkContext = newSparkSession.sparkContext
    val confCopy = conf.clone()
    val functionRegistryCopy = functionRegistry.clone()
    val sqlParser: ParserInterface = new SparkSqlParser(confCopy)
    val catalogCopy = catalog.newSessionCatalogWith(
      confCopy,
      SessionState.newHadoopConf(sparkContext.hadoopConfiguration, confCopy),
      functionRegistryCopy,
      sqlParser)
    val queryExecutionCreator = (plan: LogicalPlan) => new QueryExecution(newSparkSession, plan)

    SessionState.mergeSparkConf(confCopy, sparkContext.getConf)

    new SessionState(
      sparkContext,
      newSparkSession.sharedState,
      confCopy,
      experimentalMethods.clone(),
      functionRegistryCopy,
      catalogCopy,
      sqlParser,
      SessionState.createAnalyzer(newSparkSession, catalogCopy, confCopy),
      new StreamingQueryManager(newSparkSession),
      queryExecutionCreator)
  }

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = queryExecutionCreator(plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }

  /**
   * Add a jar path to [[SparkContext]] and the classloader.
   *
   * Note: this method seems not access any session state, but the subclass `HiveSessionState` needs
   * to add the jar to its hive client for the current session. Hence, it still needs to be in
   * [[SessionState]].
   */
  def addJar(path: String): Unit = {
    sparkContext.addJar(path)
    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    sharedState.jarClassLoader.addURL(jarURL)
    Thread.currentThread().setContextClassLoader(sharedState.jarClassLoader)
  }
}


private[sql] object SessionState {

  def apply(sparkSession: SparkSession): SessionState = {
    apply(sparkSession, new SQLConf)
  }

  def apply(sparkSession: SparkSession, sqlConf: SQLConf): SessionState = {
    val sparkContext = sparkSession.sparkContext

    // Automatically extract all entries and put them in our SQLConf
    mergeSparkConf(sqlConf, sparkContext.getConf)

    val functionRegistry = FunctionRegistry.builtin.clone()

    val sqlParser: ParserInterface = new SparkSqlParser(sqlConf)

    val catalog = new SessionCatalog(
      sparkSession.sharedState.externalCatalog,
      sparkSession.sharedState.globalTempViewManager,
      functionRegistry,
      sqlConf,
      newHadoopConf(sparkContext.hadoopConfiguration, sqlConf),
      sqlParser)

    val analyzer: Analyzer = createAnalyzer(sparkSession, catalog, sqlConf)

    val streamingQueryManager: StreamingQueryManager = new StreamingQueryManager(sparkSession)

    val queryExecutionCreator = (plan: LogicalPlan) => new QueryExecution(sparkSession, plan)

    val sessionState = new SessionState(
      sparkContext,
      sparkSession.sharedState,
      sqlConf,
      new ExperimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      analyzer,
      streamingQueryManager,
      queryExecutionCreator)
    // functionResourceLoader needs to access SessionState.addJar, so it cannot be created before
    // creating SessionState. Setting `catalog.functionResourceLoader` here is safe since the caller
    // cannot use SessionCatalog before we return SessionState.
    catalog.functionResourceLoader = sessionState.functionResourceLoader
    sessionState
  }

  def newHadoopConf(hadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val newHadoopConf = new Configuration(hadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) newHadoopConf.set(k, v) }
    newHadoopConf
  }

  /**
   * Create an logical query plan `Analyzer` with rules specific to a non-Hive `SessionState`.
   */
  private def createAnalyzer(
      sparkSession: SparkSession,
      catalog: SessionCatalog,
      sqlConf: SQLConf): Analyzer = {
    new Analyzer(catalog, sqlConf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new FindDataSourceTable(sparkSession) ::
        new ResolveSQLOnFile(sparkSession) :: Nil

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        PreprocessTableCreation(sparkSession) ::
        PreprocessTableInsertion(sqlConf) ::
        DataSourceAnalysis(sqlConf) :: Nil

      override val extendedCheckRules = Seq(PreWriteCheck, HiveOnlyCheck)
    }
  }

  /**
   * Extract entries from `SparkConf` and put them in the `SQLConf`
   */
  def mergeSparkConf(sqlConf: SQLConf, sparkConf: SparkConf): Unit = {
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
  }
}
