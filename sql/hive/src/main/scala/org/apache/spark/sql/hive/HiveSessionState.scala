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
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.{python, SparkPlanner}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.execution.HiveSqlParser
import org.apache.spark.sql.internal.{SessionState, SQLConf}


/**
 * A class that holds all session-specific state in a given [[HiveContext]].
 */
private[hive] class HiveSessionState(ctx: HiveContext) extends SessionState(ctx) {

  override lazy val conf: SQLConf = new SQLConf {
    override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
  }

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    new HiveSessionCatalog(
      ctx.hiveCatalog,
      ctx.metadataHive,
      ctx,
      ctx.functionResourceLoader,
      functionRegistry,
      conf)
  }

  /**
   * An analyzer that uses the Hive metastore.
   */
  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        catalog.CreateTables ::
        catalog.PreInsertionCasts ::
        PreInsertCastAndRename ::
        DataSourceAnalysis ::
        (if (conf.runSQLOnFile) new ResolveDataSource(ctx) :: Nil else Nil)

      override val extendedCheckRules = Seq(PreWriteCheck(conf, catalog))
    }
  }

  /**
   * Parser for HiveQl query texts.
   */
  override lazy val sqlParser: ParserInterface = HiveSqlParser

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override def planner: SparkPlanner = {
    new SparkPlanner(ctx.sparkContext, conf, experimentalMethods.extraStrategies)
      with HiveStrategies {
      override val hiveContext = ctx

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++ Seq(
          FileSourceStrategy,
          DataSourceStrategy,
          HiveCommandStrategy(ctx),
          HiveDDLStrategy,
          DDLStrategy,
          SpecialLimits,
          InMemoryScans,
          HiveTableScans,
          DataSinks,
          Scripts,
          Aggregation,
          ExistenceJoin,
          EquiJoinSelection,
          BasicOperators,
          BroadcastNestedLoop,
          CartesianProduct,
          DefaultJoin
        )
      }
    }
  }

}
