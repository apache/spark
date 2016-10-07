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

/**
 * Options for the JDBC data source.
 */
class JDBCOptions(
    @transient private val parameters: Map[String, String])
  extends Serializable {

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  require(parameters.isDefinedAt("url"), "Option 'url' is required.")
  require(parameters.isDefinedAt("dbtable"), "Option 'dbtable' is required.")
  // a JDBC URL
  val url = parameters("url")
  // name of table
  val table = parameters("dbtable")

  // ------------------------------------------------------------
  // Optional parameter list
  // ------------------------------------------------------------
  // the column used to partition
  val partitionColumn = parameters.getOrElse("partitionColumn", null)
  // the lower bound of partition column
  val lowerBound = parameters.getOrElse("lowerBound", null)
  // the upper bound of the partition column
  val upperBound = parameters.getOrElse("upperBound", null)
  // the number of partitions
  val numPartitions = parameters.getOrElse("numPartitions", null)

  require(partitionColumn == null ||
    (lowerBound != null && upperBound != null && numPartitions != null),
    "If 'partitionColumn' is specified then 'lowerBound', 'upperBound'," +
      " and 'numPartitions' are required.")

  // ------------------------------------------------------------
  // The options for DataFrameWriter
  // ------------------------------------------------------------
  // if to truncate the table from the JDBC database
  val isTruncate = parameters.getOrElse("truncate", "false").toBoolean
  // the create table option , which can be table_options or partition_options.
  // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
  // TODO: to reuse the existing partition parameters for those partition specific options
  val createTableOptions = parameters.getOrElse("createTableOptions", "")
}
