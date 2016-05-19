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

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    if (jdbcOptions.partitionColumn != null
      && (jdbcOptions.lowerBound == null
        || jdbcOptions.upperBound == null
        || jdbcOptions.numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (jdbcOptions.partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        jdbcOptions.partitionColumn,
        jdbcOptions.lowerBound.toLong,
        jdbcOptions.upperBound.toLong,
        jdbcOptions.numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    JDBCRelation(jdbcOptions.url, jdbcOptions.table, parts, properties)(sqlContext.sparkSession)
  }

  def write(mode: SaveMode, data: DataFrame, options: Map[String, String]): Unit = {
    val url = options.getOrElse("url",
      sys.error("Saving jdbc source requires url to be set." +
        " (ie. df.option(\"url\", \"ACTUAL_URL\")"))
    val table = options.getOrElse("dbtable", options.getOrElse("table",
      sys.error("Saving jdbc source requires dbtable to be set." +
        " (ie. df.option(\"dbtable\", \"ACTUAL_DB_TABLE\")")))

    import collection.JavaConverters._
    val props = new Properties()
    props.putAll(options.asJava)

    val conn = JdbcUtils.createConnectionFactory(url, props)()

    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(data, url)
        val sql = s"CREATE TABLE $table ($schema)"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } finally {
          statement.close()
        }
      }
    } finally {
      conn.close()
    }

    JdbcUtils.saveTable(data, url, table, props)
  }
}
