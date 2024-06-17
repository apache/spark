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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, DriverManager}
import java.util.{Locale, Properties}

import org.apache.commons.io.FilenameUtils

import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.TimestampNTZType
import org.apache.spark.util.Utils

/**
 * Options for the JDBC data source.
 */
class JDBCOptions(
    val parameters: CaseInsensitiveMap[String])
  extends Serializable with Logging {

  import JDBCOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  def this(url: String, table: String, parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters ++ Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> table)))
  }

  /**
   * Returns a property with all options.
   */
  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) =>
      // If an option value is `null`, throw a user-friendly error. Keys here cannot be null, as
      // scala's implementation of Maps prohibits null keys.
      if (v == null) {
        throw QueryCompilationErrors.nullDataSourceOption(k)
      }
      properties.setProperty(k, v)
    }
    properties
  }

  /**
   * Returns a property with all options except Spark internal data source options like `url`,
   * `dbtable`, and `numPartition`. This should be used when invoking JDBC API like `Driver.connect`
   * because each DBMS vendor has its own property list for JDBC driver. See SPARK-17776.
   */
  val asConnectionProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap
      .filter { case (key, _) => !jdbcOptionNames(key.toLowerCase(Locale.ROOT)) }
      .foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  require(parameters.isDefinedAt(JDBC_URL), s"Option '$JDBC_URL' is required.")
  // a JDBC URL
  val url = parameters(JDBC_URL)
  // table name or a table subquery.
  val tableOrQuery = (parameters.get(JDBC_TABLE_NAME), parameters.get(JDBC_QUERY_STRING)) match {
    case (Some(name), Some(subquery)) =>
      throw QueryExecutionErrors.cannotSpecifyBothJdbcTableNameAndQueryError(
        JDBC_TABLE_NAME, JDBC_QUERY_STRING)
    case (None, None) =>
      throw QueryExecutionErrors.missingJdbcTableNameAndQueryError(
        JDBC_TABLE_NAME, JDBC_QUERY_STRING)
    case (Some(name), None) =>
      if (name.isEmpty) {
        throw QueryExecutionErrors.emptyOptionError(JDBC_TABLE_NAME)
      } else {
        name.trim
      }
    case (None, Some(subquery)) =>
      if (subquery.isEmpty) {
        throw QueryExecutionErrors.emptyOptionError(JDBC_QUERY_STRING)
      } else {
        s"(${subquery}) SPARK_GEN_SUBQ_${curId.getAndIncrement()}"
      }
  }

  // ------------------------------------------------------------
  // Optional parameters
  // ------------------------------------------------------------
  val driverClass = {
    val userSpecifiedDriverClass = parameters.get(JDBC_DRIVER_CLASS)
    userSpecifiedDriverClass.foreach(DriverRegistry.register)

    // Performing this part of the logic on the driver guards against the corner-case where the
    // driver returned for a URL is different on the driver and executors due to classpath
    // differences.
    userSpecifiedDriverClass.getOrElse {
      DriverManager.getDriver(url).getClass.getCanonicalName
    }
  }

  // the number of partitions
  val numPartitions = parameters.get(JDBC_NUM_PARTITIONS).map(_.toInt)

  // the number of seconds the driver will wait for a Statement object to execute to the given
  // number of seconds. Zero means there is no limit.
  val queryTimeout = parameters.getOrElse(JDBC_QUERY_TIMEOUT, "0").toInt

  // ------------------------------------------------------------
  // Optional parameters only for reading
  // ------------------------------------------------------------
  // the column used to partition
  val partitionColumn = parameters.get(JDBC_PARTITION_COLUMN)
  // the lower bound of partition column
  val lowerBound = parameters.get(JDBC_LOWER_BOUND)
  // the upper bound of the partition column
  val upperBound = parameters.get(JDBC_UPPER_BOUND)
  // numPartitions is also used for data source writing
  require((partitionColumn.isEmpty && lowerBound.isEmpty && upperBound.isEmpty) ||
    (partitionColumn.isDefined && lowerBound.isDefined && upperBound.isDefined &&
      numPartitions.isDefined),
    s"When reading JDBC data sources, users need to specify all or none for the following " +
      s"options: '$JDBC_PARTITION_COLUMN', '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', " +
      s"and '$JDBC_NUM_PARTITIONS'")

  require(!(parameters.get(JDBC_QUERY_STRING).isDefined && partitionColumn.isDefined),
    s"""
       |Options '$JDBC_QUERY_STRING' and '$JDBC_PARTITION_COLUMN' can not be specified together.
       |Please define the query using `$JDBC_TABLE_NAME` option instead and make sure to qualify
       |the partition columns using the supplied subquery alias to resolve any ambiguity.
       |Example :
       |spark.read.format("jdbc")
       |  .option("url", jdbcUrl)
       |  .option("dbtable", "(select c1, c2 from t1) as subq")
       |  .option("partitionColumn", "c1")
       |  .option("lowerBound", "1")
       |  .option("upperBound", "100")
       |  .option("numPartitions", "3")
       |  .load()
     """.stripMargin
  )

  val fetchSize = parameters.getOrElse(JDBC_BATCH_FETCH_SIZE, "0").toInt

  // ------------------------------------------------------------
  // Optional parameters only for writing
  // ------------------------------------------------------------
  // if to truncate the table from the JDBC database
  val isTruncate = parameters.getOrElse(JDBC_TRUNCATE, "false").toBoolean

  val isCascadeTruncate: Option[Boolean] = parameters.get(JDBC_CASCADE_TRUNCATE).map(_.toBoolean)
  // the create table option , which can be table_options or partition_options.
  // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
  // TODO: to reuse the existing partition parameters for those partition specific options
  val createTableOptions = parameters.getOrElse(JDBC_CREATE_TABLE_OPTIONS, "")
  val createTableColumnTypes = parameters.get(JDBC_CREATE_TABLE_COLUMN_TYPES)
  val customSchema = parameters.get(JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES)

  val batchSize = {
    val size = parameters.getOrElse(JDBC_BATCH_INSERT_SIZE, "1000").toInt
    require(size >= 1,
      s"Invalid value `${size.toString}` for parameter " +
        s"`$JDBC_BATCH_INSERT_SIZE`. The minimum value is 1.")
    size
  }
  val isolationLevel =
    parameters.getOrElse(JDBC_TXN_ISOLATION_LEVEL, "READ_UNCOMMITTED") match {
      case "NONE" => Connection.TRANSACTION_NONE
      case "READ_UNCOMMITTED" => Connection.TRANSACTION_READ_UNCOMMITTED
      case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
      case "REPEATABLE_READ" => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
      case other => throw QueryExecutionErrors.invalidJdbcTxnIsolationLevelError(
        JDBC_TXN_ISOLATION_LEVEL, other)
    }
  // An option to execute custom SQL before fetching data from the remote DB
  val sessionInitStatement = parameters.get(JDBC_SESSION_INIT_STATEMENT)

  // An option to allow/disallow pushing down predicate into JDBC data source
  val pushDownPredicate = parameters.getOrElse(JDBC_PUSHDOWN_PREDICATE, "true").toBoolean

  // An option to allow/disallow pushing down aggregate into JDBC data source
  // This only applies to Data Source V2 JDBC
  val pushDownAggregate = parameters.getOrElse(JDBC_PUSHDOWN_AGGREGATE, "true").toBoolean

  // An option to allow/disallow pushing down LIMIT into V2 JDBC data source
  // This only applies to Data Source V2 JDBC
  val pushDownLimit = parameters.getOrElse(JDBC_PUSHDOWN_LIMIT, "true").toBoolean

  // An option to allow/disallow pushing down OFFSET into V2 JDBC data source
  // This only applies to Data Source V2 JDBC
  val pushDownOffset = parameters.getOrElse(JDBC_PUSHDOWN_OFFSET, "true").toBoolean

  // An option to allow/disallow pushing down TABLESAMPLE into JDBC data source
  // This only applies to Data Source V2 JDBC
  val pushDownTableSample = parameters.getOrElse(JDBC_PUSHDOWN_TABLESAMPLE, "true").toBoolean

  // The local path of user's keytab file, which is assumed to be pre-uploaded to all nodes either
  // by --files option of spark-submit or manually
  val keytab = {
    val keytabParam = parameters.getOrElse(JDBC_KEYTAB, null)
    if (keytabParam != null && FilenameUtils.getPath(keytabParam).isEmpty) {
      val result = SparkFiles.get(keytabParam)
      logDebug(s"Keytab path not found, assuming --files, file name used on executor: $result")
      result
    } else {
      logDebug("Keytab path found, assuming manual upload")
      keytabParam
    }
  }
  // The principal name of user's keytab file
  val principal = parameters.getOrElse(JDBC_PRINCIPAL, null)

  val tableComment = parameters.getOrElse(JDBC_TABLE_COMMENT, "")

  val refreshKrb5Config = parameters.getOrElse(JDBC_REFRESH_KRB5_CONFIG, "false").toBoolean

  // User specified JDBC connection provider name
  val connectionProviderName = parameters.get(JDBC_CONNECTION_PROVIDER)

  // The prefix that is added to the query sent to the JDBC database.
  // This is required to support some complex queries with some JDBC databases.
  val prepareQuery = parameters.get(JDBC_PREPARE_QUERY).map(_ + " ").getOrElse("")

  // Infers timestamp values as TimestampNTZ type when reading data.
  val preferTimestampNTZ =
    parameters
      .get(JDBC_PREFER_TIMESTAMP_NTZ)
      .map(_.toBoolean)
      .getOrElse(SQLConf.get.timestampType == TimestampNTZType)

  override def hashCode: Int = this.parameters.hashCode()

  override def equals(other: Any): Boolean = other match {
    case otherOption: JDBCOptions =>
      otherOption.parameters.equals(this.parameters)
    case _ => false
  }

  def getRedactUrl(): String = Utils.redact(SQLConf.get.stringRedactionPattern, url)
}

class JdbcOptionsInWrite(
    override val parameters: CaseInsensitiveMap[String])
  extends JDBCOptions(parameters) {

  import JDBCOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  def this(url: String, table: String, parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters ++ Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> table)))
  }

  require(
    parameters.get(JDBC_TABLE_NAME).isDefined,
    s"Option '$JDBC_TABLE_NAME' is required. " +
      s"Option '$JDBC_QUERY_STRING' is not applicable while writing.")

  val table = parameters(JDBC_TABLE_NAME)
}

object JDBCOptions {
  private val curId = new java.util.concurrent.atomic.AtomicLong(0L)
  private val jdbcOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    jdbcOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val JDBC_URL = newOption("url")
  val JDBC_TABLE_NAME = newOption("dbtable")
  val JDBC_QUERY_STRING = newOption("query")
  val JDBC_DRIVER_CLASS = newOption("driver")
  val JDBC_PARTITION_COLUMN = newOption("partitionColumn")
  val JDBC_LOWER_BOUND = newOption("lowerBound")
  val JDBC_UPPER_BOUND = newOption("upperBound")
  val JDBC_NUM_PARTITIONS = newOption("numPartitions")
  val JDBC_QUERY_TIMEOUT = newOption("queryTimeout")
  val JDBC_BATCH_FETCH_SIZE = newOption("fetchsize")
  val JDBC_TRUNCATE = newOption("truncate")
  val JDBC_CASCADE_TRUNCATE = newOption("cascadeTruncate")
  val JDBC_CREATE_TABLE_OPTIONS = newOption("createTableOptions")
  val JDBC_CREATE_TABLE_COLUMN_TYPES = newOption("createTableColumnTypes")
  val JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES = newOption("customSchema")
  val JDBC_BATCH_INSERT_SIZE = newOption("batchsize")
  val JDBC_TXN_ISOLATION_LEVEL = newOption("isolationLevel")
  val JDBC_SESSION_INIT_STATEMENT = newOption("sessionInitStatement")
  val JDBC_PUSHDOWN_PREDICATE = newOption("pushDownPredicate")
  val JDBC_PUSHDOWN_AGGREGATE = newOption("pushDownAggregate")
  val JDBC_PUSHDOWN_LIMIT = newOption("pushDownLimit")
  val JDBC_PUSHDOWN_OFFSET = newOption("pushDownOffset")
  val JDBC_PUSHDOWN_TABLESAMPLE = newOption("pushDownTableSample")
  val JDBC_KEYTAB = newOption("keytab")
  val JDBC_PRINCIPAL = newOption("principal")
  val JDBC_TABLE_COMMENT = newOption("tableComment")
  val JDBC_REFRESH_KRB5_CONFIG = newOption("refreshKrb5Config")
  val JDBC_CONNECTION_PROVIDER = newOption("connectionProvider")
  val JDBC_PREPARE_QUERY = newOption("prepareQuery")
  val JDBC_PREFER_TIMESTAMP_NTZ = newOption("preferTimestampNTZ")
}
