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

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{ArchiveResource, _}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.AnalyzeTable
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, PreInsertCastAndRename, ResolveDataSource}
import org.apache.spark.sql.util.ExecutionListenerManager


/**
 * A class that holds all session-specific state in a given [[SQLContext]].
 */
private[sql] class SessionState(ctx: SQLContext) {

  // Note: These are all lazy vals because they depend on each other (e.g. conf) and we
  // want subclasses to override some of the fields. Otherwise, we would get a lot of NPEs.

  /**
   * SQL-specific key-value configurations.
   */
  lazy val conf: SQLConf = new SQLConf

  // Automatically extract `spark.sql.*` entries and put it in our SQLConf
  setConf(SQLContext.getSQLProperties(ctx.sparkContext.getConf))

  lazy val experimentalMethods = new ExperimentalMethods

  /**
   * Internal catalog for managing functions registered by the user.
   */
  lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin.copy()

  /**
   * A class for loading resources specified by a function.
   */
  lazy val functionResourceLoader: FunctionResourceLoader = {
    new FunctionResourceLoader {
      override def loadResource(resource: FunctionResource): Unit = {
        resource.resourceType match {
          case JarResource => addJar(resource.uri)
          case FileResource => ctx.sparkContext.addFile(resource.uri)
          case ArchiveResource =>
            throw new AnalysisException(
              "Archive is not allowed to be loaded. If YARN mode is used, " +
                "please use --archives options while calling spark-submit.")
        }
      }
    }
  }

  /**
   * Internal catalog for managing table and database states.
   */
  lazy val catalog =
    new SessionCatalog(
      ctx.externalCatalog,
      functionResourceLoader,
      functionRegistry,
      conf)

  /**
   * Interface exposed to the user for registering user-defined functions.
   */
  lazy val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  /**
   * Logical query plan analyzer for resolving unresolved attributes and relations.
   */
  lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        PreInsertCastAndRename ::
        DataSourceAnalysis ::
        (if (conf.runSQLonFile) new ResolveDataSource(ctx) :: Nil else Nil)

      override val extendedCheckRules = Seq(datasources.PreWriteCheck(conf, catalog))
    }
  }

  /**
   * Logical query plan optimizer.
   */
  lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods)

  /**
   * Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
   */
  lazy val sqlParser: ParserInterface = new SparkSqlParser(conf)

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  def planner: SparkPlanner =
    new SparkPlanner(ctx.sparkContext, conf, experimentalMethods.extraStrategies)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  lazy val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  /**
   * Interface to start and stop [[org.apache.spark.sql.ContinuousQuery]]s.
   */
  lazy val continuousQueryManager: ContinuousQueryManager = new ContinuousQueryManager(ctx)


  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = new QueryExecution(ctx, plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }

  def invalidateTable(tableName: String): Unit = {
    catalog.invalidateTable(sqlParser.parseTableIdentifier(tableName))
  }

  final def setConf(properties: Properties): Unit = {
    properties.asScala.foreach { case (k, v) => setConf(k, v) }
  }

  final def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    conf.setConf(entry, value)
    setConf(entry.key, entry.stringConverter(value))
  }

  def setConf(key: String, value: String): Unit = {
    conf.setConfString(key, value)
  }

  def addJar(path: String): Unit = {
    ctx.sparkContext.addJar(path)
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports catalog tables and it only updates the size of a catalog table
   * in the external catalog.
   */
  def analyze(tableName: String): Unit = {
    AnalyzeTable(tableName).run(ctx)
  }

  def runNativeSql(sql: String): Seq[String] = {
    throw new AnalysisException("Unsupported query: " + sql)
  }

}
