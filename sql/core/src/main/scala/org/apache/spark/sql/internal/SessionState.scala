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
import org.apache.spark.annotation.{Experimental, InterfaceStability}
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
 *
 * @param sparkContext The [[SparkContext]].
 * @param sharedState The shared state.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods The experimental methods.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param catalog Internal catalog for managing table and database states.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param analyzer Logical query plan analyzer for resolving unresolved attributes and relations.
 * @param optimizer Logical query plan optimizer.
 * @param planner Planner that converts optimized logical plans to physical plans
 * @param streamingQueryManager Interface to start and stop streaming queries.
 * @param createQueryExecution Function used to create QueryExecution objects.
 * @param createClone Function used to create clones of the session state.
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
    val optimizer: Optimizer,
    val planner: SparkPlanner,
    val streamingQueryManager: StreamingQueryManager,
    createQueryExecution: LogicalPlan => QueryExecution,
    createClone: (SparkSession, SessionState) => SessionState) {

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
   * Interface exposed to the user for registering user-defined functions.
   * Note that the user-defined functions must be deterministic.
   */
  val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def clone(newSparkSession: SparkSession): SessionState = createClone(newSparkSession, this)

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = createQueryExecution(plan)

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
  /**
   * Create a new [[SessionState]] for the given session.
   */
  def apply(session: SparkSession): SessionState = {
    new SessionStateBuilder(session).build()
  }

  def newHadoopConf(hadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val newHadoopConf = new Configuration(hadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) newHadoopConf.set(k, v) }
    newHadoopConf
  }
}

/**
 * Builder class that coordinates construction of a new [[SessionState]].
 *
 * The builder explicitly defines all components needed by the session state, and creates a session
 * state when `build` is called. Components should only be initialized once. This is not a problem
 * for most components as they are only used in the `build` function. However some components
 * (`conf`, `catalog`, `functionRegistry`, `experimentalMethods` & `sqlParser`) are as dependencies
 * for other components and are shared as a result. These components are defined as lazy vals to
 * make sure the component is created only once.
 *
 * A developer can modify the builder by providing custom versions of components, or by using the
 * hooks provided for the analyzer, optimizer & planner. There are some dependencies between the
 * components (they are documented per dependency), a developer should respect these when making
 * modifications in order to prevent initialization problems.
 *
 * A parent [[SessionState]] can be used to initialize the new [[SessionState]]. The new session
 * state will clone the parent sessions state's `conf`, `functionRegistry`, `experimentalMethods`
 * and `catalog` fields. Note that the state is cloned when `build` is called, and not before.
 */
@Experimental
@InterfaceStability.Unstable
abstract class BaseSessionStateBuilder(
    val session: SparkSession,
    val parentState: Option[SessionState] = None) {
  type NewBuilder = (SparkSession, Option[SessionState]) => BaseSessionStateBuilder

  /**
   * Extract entries from `SparkConf` and put them in the `SQLConf`
   */
  protected def mergeSparkConf(sqlConf: SQLConf, sparkConf: SparkConf): Unit = {
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
  }

  /**
   * SQL-specific key-value configurations.
   *
   * These either get cloned from a pre-existing instance or newly created. The conf is always
   * merged with its [[SparkConf]].
   */
  protected lazy val conf: SQLConf = {
    val conf = parentState.map(_.conf.clone()).getOrElse(new SQLConf)
    mergeSparkConf(conf, session.sparkContext.conf)
    conf
  }

  /**
   * Internal catalog managing functions registered by the user.
   *
   * This either gets cloned from a pre-existing version or cloned from the build-in registry.
   */
  protected lazy val functionRegistry: FunctionRegistry = {
    parentState.map(_.functionRegistry).getOrElse(FunctionRegistry.builtin).clone()
  }

  /**
   * Experimental methods that can be used to define custom optimization rules and custom planning
   * strategies.
   *
   * This either gets cloned from a pre-existing version or newly created.
   */
  protected lazy val experimentalMethods: ExperimentalMethods = {
    parentState.map(_.experimentalMethods.clone()).getOrElse(new ExperimentalMethods)
  }

  /**
   * Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
   *
   * Note: this depends on the `conf` field.
   */
  protected lazy val sqlParser: ParserInterface = new SparkSqlParser(conf)

  /**
   * Catalog for managing table and database states. If there is a pre-existing catalog, the state
   * of that catalog (temp tables & current database) will be copied into the new catalog.
   *
   * Note: this depends on the `conf`, `functionRegistry` and `sqlParser` fields.
   */
  protected lazy val catalog: SessionCatalog = {
    val catalog = new SessionCatalog(
      session.sharedState.externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      new SessionFunctionResourceLoader(session))
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  /**
   * Logical query plan analyzer for resolving unresolved attributes and relations.
   *
   * Note: this depends on the `conf` and `catalog` fields.
   */
  protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new FindDataSourceTable(session) +:
      new ResolveSQLOnFile(session) +:
      customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      PreprocessTableCreation(session) +:
      PreprocessTableInsertion(conf) +:
      DataSourceAnalysis(conf) +:
      customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
      HiveOnlyCheck +:
      customCheckRules
  }

  /**
   * Custom resolution rules to add to the Analyzer. Prefer overriding this instead of creating
   * your own Analyzer.
   *
   * Note that this may NOT depend on the `analyzer` function.
   */
  protected def customResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Custom post resolution rules to add to the Analyzer. Prefer overriding this instead of
   * creating your own Analyzer.
   *
   * Note that this may NOT depend on the `analyzer` function.
   */
  protected def customPostHocResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Custom check rules to add to the Analyzer. Prefer overriding this instead of creating
   * your own Analyzer.
   *
   * Note that this may NOT depend on the `analyzer` function.
   */
  protected def customCheckRules: Seq[LogicalPlan => Unit] = Nil

  /**
   * Logical query plan optimizer.
   *
   * Note: this depends on the `conf`, `catalog` and `experimentalMethods` fields.
   */
  protected def optimizer: Optimizer = {
    new SparkOptimizer(catalog, conf, experimentalMethods) {
      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules
    }
  }

  /**
   * Custom operator optimization rules to add to the Optimizer. Prefer overriding this instead
   * of creating your own Optimizer.
   *
   * Note that this may NOT depend on the `optimizer` function.
   */
  protected def customOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Planner that converts optimized logical plans to physical plans.
   *
   * Note: this depends on the `conf` and `experimentalMethods` fields.
   */
  protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods) {
      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies
    }
  }

  /**
   * Custom strategies to add to the planner. Prefer overriding this instead of creating
   * your own Planner.
   *
   * Note that this may NOT depend on the `planner` function.
   */
  protected def customPlanningStrategies: Seq[Strategy] = Nil

  /**
   * Create a query execution object.
   */
  protected def createQueryExecution: LogicalPlan => QueryExecution = { plan =>
    new QueryExecution(session, plan)
  }

  /**
   * Interface to start and stop streaming queries.
   */
  protected def streamingQueryManager: StreamingQueryManager = new StreamingQueryManager(session)

  /**
   * Function that produces a new instance of the SessionStateBuilder. This is used by the
   * [[SessionState]]'s clone functionality. Make sure to override this when implementing your own
   * [[SessionStateBuilder]].
   */
  protected def newBuilder: NewBuilder

  /**
   * Function used to make clones of the session state.
   */
  protected def createClone: (SparkSession, SessionState) => SessionState = {
    val createBuilder = newBuilder
    (session, state) => createBuilder(session, Option(state)).build()
  }

  /**
   * Build the [[SessionState]].
   */
  def build(): SessionState = {
    new SessionState(
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
      createClone)
  }
}

/**
 * Concrete implementation of a [[SessionStateBuilder]].
 */
@Experimental
@InterfaceStability.Unstable
class SessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {
  override protected def newBuilder: NewBuilder = new SessionStateBuilder(_, _)
}

/**
 * Helper class for using SessionStateBuilders during tests.
 */
private[sql] trait WithTestConf { self: BaseSessionStateBuilder =>
  def overrideConfs: Map[String, String]

  override protected lazy val conf: SQLConf = {
    val conf = parentState.map(_.conf.clone()).getOrElse {
      new SQLConf {
        clear()
        override def clear(): Unit = {
          super.clear()
          // Make sure we start with the default test configs even after clear
          overrideConfs.foreach { case (key, value) => setConfString(key, value) }
        }
      }
    }
    mergeSparkConf(conf, session.sparkContext.conf)
    conf
  }
}

/**
 * Session shared [[FunctionResourceLoader]].
 */
@InterfaceStability.Unstable
class SessionFunctionResourceLoader(session: SparkSession) extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    resource.resourceType match {
      case JarResource => session.sessionState.addJar(resource.uri)
      case FileResource => session.sparkContext.addFile(resource.uri)
      case ArchiveResource =>
        throw new AnalysisException(
          "Archive is not allowed to be loaded. If YARN mode is used, " +
            "please use --archives options while calling spark-submit.")
    }
  }
}
