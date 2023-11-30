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

package org.apache.spark.sql.crossdbms

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry

/**
 * Represents a connection (session) to a database.
 */
private[sql] trait JdbcConnection {
  /**
   * Runs the given query.
   * @return A Seq[String] representing the output. This is a Seq where each element represents a
   *         single row.
   */
  def runQuery(query: String): Seq[String]

  /**
   * Drop the table with the given table name.
   */
  def dropTable(tableName: String): Unit

  /**
   * Create a table with the given table name and schema.
   */
  def createTable(tableName: String, schemaString: String): Unit

  /**
   * Load data from the given Spark Dataframe into the table with given name.
   */
  def loadData(df: DataFrame, tableName: String): Unit

  /**
   * Close the connection.
   */
  def close(): Unit
}

/**
 * Represents a connection (session) to a PostgreSQL database.
 */
private[sql] case class PostgresConnection(connection_url: Option[String] = None)
  extends JdbcConnection {

  DriverRegistry.register("org.postgresql.Driver")
  private final val DEFAULT_USER = "pg"
  private final val DEFAULT_CONNECTION_URL =
    s"jdbc:postgresql://localhost:5432/postgres?user=$DEFAULT_USER"
  private val url = connection_url.getOrElse(DEFAULT_CONNECTION_URL)
  private val conn = DriverManager.getConnection(url)
  private val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

  def runQuery(query: String): Seq[String] = {
    try {
      val isResultSet = stmt.execute(query)
      val rows = ArrayBuffer[Row]()
      if (isResultSet) {
        val rs = stmt.getResultSet
        val metadata = rs.getMetaData
        while (rs.next()) {
          val row = Row.fromSeq((1 to metadata.getColumnCount).map(i => rs.getObject(i)))
          rows.append(row)
        }
      }
      rows.map(_.mkString("\t")).toSeq
    } catch {
      case e: Throwable => Seq(e.toString)
    }
  }

  def dropTable(tableName: String): Unit = {
    val dropTableSql = s"DROP TABLE IF EXISTS $tableName"
    stmt.executeUpdate(dropTableSql)
  }

  def createTable(tableName: String, schemaString: String): Unit = {
    val createTableSql = s"CREATE TABLE $tableName ($schemaString)"
    stmt.executeUpdate(createTableSql)
  }

  def loadData(df: DataFrame, tableName: String): Unit = {
    df.write.option("driver", "org.postgresql.Driver").mode("append")
      .jdbc(url, tableName, new Properties())
  }

  def close(): Unit = if (!conn.isClosed) conn.close()
}
