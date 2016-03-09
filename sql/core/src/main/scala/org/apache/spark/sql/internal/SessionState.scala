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

import org.apache.spark.sql.{ContinuousQueryManager, SQLContext, UDFRegistration}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, Catalog, FunctionRegistry, SimpleCatalog}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, PreInsertCastAndRename, ResolveDataSource}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
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
  lazy val conf = new SQLConf

  /**
   * Internal catalog for managing table and database states.
   */
  lazy val catalog: Catalog = new SimpleCatalog(conf)

  /**
   * Internal catalog for managing functions registered by the user.
   */
  lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin.copy()

  /**
   * Interface exposed to the user for registering user-defined functions.
   */
  lazy val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  /**
   * Logical query plan analyzer for resolving unresolved attributes and relations.
   */
  lazy val analyzer: Analyzer = {
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        python.ExtractPythonUDFs ::
        PreInsertCastAndRename ::
        DataSourceAnalysis ::
        (if (conf.runSQLOnFile) new ResolveDataSource(ctx) :: Nil else Nil)

      override val extendedCheckRules = Seq(datasources.PreWriteCheck(catalog))
    }
  }

  /**
   * Logical query plan optimizer.
   */
  lazy val optimizer: Optimizer = new SparkOptimizer(ctx)

  /**
   * Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
   */
  lazy val sqlParser: ParserInterface = new SparkQl(conf)

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  lazy val planner: SparkPlanner = new SparkPlanner(ctx)

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  lazy val prepareForExecution = new RuleExecutor[SparkPlan] {
    override val batches: Seq[Batch] = Seq(
      Batch("Subquery", Once, PlanSubqueries(ctx)),
      Batch("Add exchange", Once, EnsureRequirements(ctx)),
      Batch("Whole stage codegen", Once, CollapseCodegenStages(ctx)),
      Batch("Reuse duplicated exchanges", Once, ReuseExchange(ctx))
    )
  }

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  lazy val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  /**
   * Interface to start and stop [[org.apache.spark.sql.ContinuousQuery]]s.
   */
  lazy val continuousQueryManager: ContinuousQueryManager = new ContinuousQueryManager(ctx)

}
