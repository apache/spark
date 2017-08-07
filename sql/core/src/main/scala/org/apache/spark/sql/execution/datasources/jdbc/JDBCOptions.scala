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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the JDBC data source.
 */
class JDBCOptions(
    @transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

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
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  /**
   * Returns a property with all options except Spark internal data source options like `url`,
   * `dbtable`, and `numPartition`. This should be used when invoking JDBC API like `Driver.connect`
   * because each DBMS vendor has its own property list for JDBC driver. See SPARK-17776.
   */
  val asConnectionProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.filterKeys(key => !jdbcOptionNames(key.toLowerCase(Locale.ROOT)))
      .foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  require(parameters.isDefinedAt(JDBC_URL), s"Option '$JDBC_URL' is required.")
  require(parameters.isDefinedAt(JDBC_TABLE_NAME), s"Option '$JDBC_TABLE_NAME' is required.")
  // a JDBC URL
  val url = parameters(JDBC_URL)
  // name of table
  val table = parameters(JDBC_TABLE_NAME)

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

  // ------------------------------------------------------------
  // Optional parameters only for reading
  // ------------------------------------------------------------
  // the column used to partition
  val partitionColumn = parameters.get(JDBC_PARTITION_COLUMN)
  // the lower bound of partition column
  val lowerBound = parameters.get(JDBC_LOWER_BOUND).map(_.toLong)
  // the upper bound of the partition column
  val upperBound = parameters.get(JDBC_UPPER_BOUND).map(_.toLong)
  // numPartitions is also used for data source writing
  require((partitionColumn.isEmpty && lowerBound.isEmpty && upperBound.isEmpty) ||
    (partitionColumn.isDefined && lowerBound.isDefined && upperBound.isDefined &&
      numPartitions.isDefined),
    s"When reading JDBC data sources, users need to specify all or none for the following " +
      s"options: '$JDBC_PARTITION_COLUMN', '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', " +
      s"and '$JDBC_NUM_PARTITIONS'")
  val fetchSize = {
    val size = parameters.getOrElse(JDBC_BATCH_FETCH_SIZE, "0").toInt
    require(size >= 0,
      s"Invalid value `${size.toString}` for parameter " +
        s"`$JDBC_BATCH_FETCH_SIZE`. The minimum value is 0. When the value is 0, " +
        "the JDBC driver ignores the value and does the estimates.")
    size
  }

  // ------------------------------------------------------------
  // Optional parameters only for writing
  // ------------------------------------------------------------
  // if to truncate the table from the JDBC database
  val isTruncate = parameters.getOrElse(JDBC_TRUNCATE, "false").toBoolean
  // the create table option , which can be table_options or partition_options.
  // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
  // TODO: to reuse the existing partition parameters for those partition specific options
  val createTableOptions = parameters.getOrElse(JDBC_CREATE_TABLE_OPTIONS, "")
  val createTableColumnTypes = parameters.get(JDBC_CREATE_TABLE_COLUMN_TYPES)
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
    }
}

object JDBCOptions {
  private val jdbcOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    jdbcOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val JDBC_URL = newOption("url")
  val JDBC_TABLE_NAME = newOption("dbtable")
  val JDBC_DRIVER_CLASS = newOption("driver")
  val JDBC_PARTITION_COLUMN = newOption("partitionColumn")
  val JDBC_LOWER_BOUND = newOption("lowerBound")
  val JDBC_UPPER_BOUND = newOption("upperBound")
  val JDBC_NUM_PARTITIONS = newOption("numPartitions")
  val JDBC_BATCH_FETCH_SIZE = newOption("fetchsize")
  val JDBC_TRUNCATE = newOption("truncate")
  val JDBC_CREATE_TABLE_OPTIONS = newOption("createTableOptions")
  val JDBC_CREATE_TABLE_COLUMN_TYPES = newOption("createTableColumnTypes")
  val JDBC_BATCH_INSERT_SIZE = newOption("batchsize")
  val JDBC_TXN_ISOLATION_LEVEL = newOption("isolationLevel")
}
