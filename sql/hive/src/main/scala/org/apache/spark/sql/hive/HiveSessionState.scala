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

import java.util.regex.Pattern

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.{HiveClient, HiveClientImpl}
import org.apache.spark.sql.hive.execution.HiveSqlParser
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.DataType


/**
 * A class that holds all session-specific state in a given [[HiveContext]].
 */
private[hive] class HiveSessionState(ctx: SQLContext) extends SessionState(ctx) {

  val hivePersistentState = ctx.sharedState.asInstanceOf[HiveSharedState]

  /**
   * A Hive client used for execution.
   */
  val executionHive: HiveClientImpl = hivePersistentState.executionHive.newSession()

  /**
   * A Hive client used for interacting with the metastore.
   */
  val metadataHive: HiveClient = hivePersistentState.metadataHive.newSession()

  override lazy val conf: SQLConf = new SQLConf {
    override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
  }

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    new HiveSessionCatalog(
      ctx.externalCatalog,
      metadataHive,
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
      override val hiveContext = ctx.asInstanceOf[HiveContext]

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

  // All of stuff moved from HiveContext

  lazy val hiveconf: HiveConf = {
    val c = executionHive.conf
    ctx.setConf(c.getAllProperties)
    c
  }

  @transient
  protected[sql] lazy val substitutor = new VariableSubstitution()

  override def withSubstitution(sql: String): String = {
    substitutor.substitute(hiveconf, sql)
  }

  override def defaultOverrides(): Unit = {
    ctx.setConf(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS.varname, "false")
  }

  private def functionOrMacroDDLPattern(command: String) = Pattern.compile(
    ".*(create|drop)\\s+(temporary\\s+)?(function|macro).+", Pattern.DOTALL).matcher(command)

  override def runSqlHive(sql: String): Seq[String] = {
    val command = sql.trim.toLowerCase
    if (functionOrMacroDDLPattern(command).matches()) {
      executionHive.runSqlHive(sql)
    } else if (command.startsWith("set")) {
      metadataHive.runSqlHive(sql)
      executionHive.runSqlHive(sql)
    } else {
      metadataHive.runSqlHive(sql)
    }
  }

  override def setConfHook(key: String, value: String): Unit = {
    super.setConfHook(key, value)
    executionHive.runSqlHive(s"SET $key=$value")
    metadataHive.runSqlHive(s"SET $key=$value")
    // If users put any Spark SQL setting in the spark conf (e.g. spark-defaults.conf),
    // this setConf will be called in the constructor of the SQLContext.
    // Also, calling hiveconf will create a default session containing a HiveConf, which
    // will interfer with the creation of executionHive (which is a lazy val). So,
    // we put hiveconf.set at the end of this method.
    hiveconf.set(key, value)
  }

  override def addJarHook(path: String): Unit = {
    // Add jar to Hive and classloader
    executionHive.addJar(path)
    metadataHive.addJar(path)
    Thread.currentThread().setContextClassLoader(executionHive.clientLoader.classLoader)
    super.addJarHook(path)
  }

  override def formatStringResult(a: (Any, DataType)): String = {
    HiveContext.toHiveString(a)
  }
}
