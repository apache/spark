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

import java.sql.SQLException
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class JdbcRelationProvider extends CreatableRelationProvider
  with SchemaRelationProvider with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val partitionColumn = jdbcOptions.partitionColumn
    val lowerBound = jdbcOptions.lowerBound
    val upperBound = jdbcOptions.upperBound
    val numPartitions = jdbcOptions.numPartitions

    val partitionInfo = if (partitionColumn == null) null
    else {
      JDBCPartitioningInfo(
        partitionColumn, lowerBound.toLong, upperBound.toLong, numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    JDBCRelation(jdbcOptions.url, jdbcOptions.table, parts, properties,
      Option(schema))(sqlContext.sparkSession)
  }

  /*
   * The following structure applies to this code:
   *                 |    tableExists            |          !tableExists
   *------------------------------------------------------------------------------------
   * Ignore          | BaseRelation              | CreateTable, saveTable, BaseRelation
   * ErrorIfExists   | ERROR                     | CreateTable, saveTable, BaseRelation
   * Overwrite       | DropTable, CreateTable,   | CreateTable, saveTable, BaseRelation
   *                 | saveTable, BaseRelation   |
   * Append          | saveTable, BaseRelation   | CreateTable, saveTable, BaseRelation
   */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    require(parameters.isDefinedAt("url"), "Saving jdbc source requires 'url' to be set." +
        " (ie. df.option(\"url\", \"ACTUAL_URL\")")
    require(parameters.isDefinedAt("dbtable"), "Saving jdbc source requires 'dbtable' to be set." +
        " (ie. df.option(\"dbtable\", \"ACTUAL_DB_TABLE\")")
    val url = parameters("url")
    val table = parameters("dbtable")

    import collection.JavaConverters._
    val props = new Properties()
    props.putAll(parameters.asJava)
    val conn = JdbcUtils.createConnectionFactory(url, props)()

    try {
      val tableExists = JdbcUtils.tableExists(conn, url, table)

      val (doCreate, doSave) = (mode, tableExists) match {
        case (SaveMode.Ignore, true) => (false, false)
        case (SaveMode.ErrorIfExists, true) => throw new SQLException(
          s"Table $table already exists, and SaveMode is set to ErrorIfExists.")
        case (SaveMode.Overwrite, true) =>
          JdbcUtils.dropTable(conn, table)
          (true, true)
        case (SaveMode.Append, true) => (false, true)
        case (_, true) => throw new IllegalArgumentException(s"Unexpected SaveMode, '$mode'," +
          " for handling existing tables.")
        case (_, false) => (true, true)
      }

      if(doCreate) {
        val schema = JdbcUtils.schemaString(data, url)
        val sql = s"CREATE TABLE $table ($schema)"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } finally {
          statement.close()
        }
      }
      if(doSave) JdbcUtils.saveTable(data, url, table, props)
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
