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

package org.apache.spark.sql.execution.datasources.jdbc.jdbcv2

import java.util.{NoSuchElementException, Properties}
import java.util.function.Supplier

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.v2.DataSourceOptions

class JDBCOptionsV2(val options: DataSourceOptions) extends Serializable {

  import JDBCOptionsV2._

  def this() = this(new DataSourceOptions(Map.empty[String, String].asJava))

  def this(parameters: Map[String, String]) = this(new DataSourceOptions(parameters.asJava))

  // a JDBC URL
  val url = options.get(JDBC_URL).orElseThrow(new Supplier[Throwable] {
    override def get(): Throwable = new NoSuchElementException("no such url")
  })

  // TODO use dataSourceOptions.tableName()
  val table = options.get(JDBC_TABLE_NAME).orElseThrow(new Supplier[Throwable] {
    override def get(): Throwable = new NoSuchElementException("no such table")
  })

  // TODO register user specified driverClass
  val driverClass = "org.h2.Driver"

  val asConnectionProperties: Properties = {
    val properties = new Properties()
    properties
  }

  val partitionColumn = toScala(options.get(JDBC_PARTITION_COLUMN))
  val lowerBound = options.getLong(JDBC_LOWER_BOUND, Long.MaxValue)
  val upperBound = options.getLong(JDBC_UPPER_BOUND, Long.MinValue)
  val numPartitions = options.getInt(JDBC_NUM_PARTITIONS, 1)
  val fetchSize = options.getInt(JDBC_BATCH_FETCH_SIZE, 1000)
  val queryTimeout = options.getInt(JDBC_BATCH_FETCH_SIZE, 60)
  val predicates = toScala(options.get(JDBC_Predicates))

  // Convert info to V1
  val jdbcOptionsV1 = new JDBCOptions(Map(JDBCOptions.JDBC_URL -> url,
    JDBCOptions.JDBC_TABLE_NAME -> table, // databaseName + "," + table)
    JDBCOptions.JDBC_DRIVER_CLASS -> driverClass)
  )

}

object JDBCOptionsV2 {

  val JDBC_URL = "url"
  val JDBC_DATABASE_NAME = "database"
  val JDBC_TABLE_NAME = "dbtable"
  val JDBC_DRIVER_CLASS = "driver"
  val JDBC_PARTITION_COLUMN = "partitionColumn"
  val JDBC_LOWER_BOUND = "lowerBound"
  val JDBC_UPPER_BOUND = "upperBound"
  val JDBC_NUM_PARTITIONS = "numPartitions"
  val JDBC_Predicates = "predicates"
  val JDBC_QUERY_TIMEOUT = "queryTimeout"
  val JDBC_BATCH_FETCH_SIZE = "fetchsize"
  val JDBC_TRUNCATE = "truncate"
  val JDBC_CREATE_TABLE_OPTIONS = "createTableOptions"
  val JDBC_CREATE_TABLE_COLUMN_TYPES = "createTableColumnTypes"
  val JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES = "customSchema"
  val JDBC_BATCH_INSERT_SIZE = "batchsize"
  val JDBC_TXN_ISOLATION_LEVEL = "isolationLevel"
  val JDBC_SESSION_INIT_STATEMENT = "sessionInitStatement"

  final def toScala[A](o: java.util.Optional[A]): Option[A] = if (o.isPresent) Some(o.get) else None
}
