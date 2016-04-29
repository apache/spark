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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.SessionState


/**
 * A class that holds all session-specific state in a given [[SparkSession]] backed by Hive.
 */
private[hive] class HiveSessionState(sparkSession: SparkSession)
  extends SessionState(sparkSession) {

  self =>

  private lazy val sharedState: HiveSharedState = {
    sparkSession.sharedState.asInstanceOf[HiveSharedState]
  }

  /**
   * A Hive client used for interacting with the metastore.
   */
  lazy val metadataHive: HiveClient = sharedState.metadataHive.newSession()

  /**
   * SQLConf and HiveConf contracts:
   *
   * 1. create a new o.a.h.hive.ql.session.SessionState for each HiveContext
   * 2. when the Hive session is first initialized, params in HiveConf will get picked up by the
   *    SQLConf.  Additionally, any properties set by set() or a SET command inside sql() will be
   *    set in the SQLConf *as well as* in the HiveConf.
   */
  lazy val hiveconf: HiveConf = {
    val initialConf = new HiveConf(
      sparkSession.sparkContext.hadoopConfiguration,
      classOf[org.apache.hadoop.hive.ql.session.SessionState])

    // HiveConf is a Hadoop Configuration, which has a field of classLoader and
    // the initial value will be the current thread's context class loader
    // (i.e. initClassLoader at here).
    // We call initialConf.setClassLoader(initClassLoader) at here to make
    // this action explicit.
    initialConf.setClassLoader(sparkSession.sharedState.jarClassLoader)
    sparkSession.sparkContext.conf.getAll.foreach { case (k, v) =>
      initialConf.set(k, v)
    }
    initialConf
  }

  setDefaultOverrideConfs()

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    new HiveSessionCatalog(
      sharedState.externalCatalog,
      metadataHive,
      sparkSession,
      functionResourceLoader,
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
        (if (conf.runSQLonFile) new ResolveDataSource(sparkSession) :: Nil else Nil)

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
          DataSinks,
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

  /**
   * Overrides default Hive configurations to avoid breaking changes to Spark SQL users.
   *  - allow SQL11 keywords to be used as identifiers
   */
  def setDefaultOverrideConfs(): Unit = {
    setConf(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS.varname, "false")
  }

  override def setConf(key: String, value: String): Unit = {
    super.setConf(key, value)
    metadataHive.runSqlHive(s"SET $key=$value")
    hiveconf.set(key, value)
  }

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
   * When true, a table created by a Hive CTAS statement (no USING clause) will be
   * converted to a data source table, using the data source set by spark.sql.sources.default.
   * The table in CTAS statement will be converted when it meets any of the following conditions:
   *   - The CTAS does not specify any of a SerDe (ROW FORMAT SERDE), a File Format (STORED AS), or
   *     a Storage Hanlder (STORED BY), and the value of hive.default.fileformat in hive-site.xml
   *     is either TextFile or SequenceFile.
   *   - The CTAS statement specifies TextFile (STORED AS TEXTFILE) as the file format and no SerDe
   *     is specified (no ROW FORMAT SERDE clause).
   *   - The CTAS statement specifies SequenceFile (STORED AS SEQUENCEFILE) as the file format
   *     and no SerDe is specified (no ROW FORMAT SERDE clause).
   */
  def convertCTAS: Boolean = {
    conf.getConf(HiveUtils.CONVERT_CTAS)
  }

  /**
   * When true, Hive Thrift server will execute SQL queries asynchronously using a thread pool."
   */
  def hiveThriftServerAsync: Boolean = {
    conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
  }

  // TODO: why do we get this from SparkConf but not SQLConf?
  def hiveThriftServerSingleSession: Boolean = {
    sparkSession.sparkContext.conf.getBoolean(
      "spark.sql.hive.thriftServer.singleSession", defaultValue = false)
  }

}
