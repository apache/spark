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
 * Represents a connection (session) to a database using JDBC.
 */
private[crossdbms] trait JdbcConnection {

  /**
   * Executes the given SQL query and returns the result as a sequence of strings.
   * @return A Seq[String] representing the output, where each element represents a single row.
   */
  def runQuery(query: String): Seq[String]

  /**
   * Drops the table with the specified table name.
   */
  def dropTable(tableName: String): Unit

  /**
   * Creates a table with the specified name and schema.
   * @param schemaString The schema definition for the table. Note that this may vary depending on
   *                     the database system.
   */
  def createTable(tableName: String, schemaString: String): Unit

  /**
   * Loads data from the given Spark DataFrame into the table with the specified name.
   * @param df        The Spark DataFrame containing the data to be loaded.
   */
  def loadData(df: DataFrame, tableName: String): Unit

  /**
   * Closes the JDBC connection.
   */
  def close(): Unit
}

/**
 * Represents a connection (session) to a PostgreSQL database.
 */
private[crossdbms] case class PostgresConnection(connection_url: Option[String] = None)
  extends JdbcConnection {

  private final val POSTGRES_DRIVER_CLASS_NAME = "org.postgresql.Driver"
  DriverRegistry.register(POSTGRES_DRIVER_CLASS_NAME)

  private final val DEFAULT_HOST = "localhost"
  private final val DEFAULT_PORT = "5432"
  private final val DEFAULT_USER = "postgres"
  private final val DEFAULT_DB = "postgres"
  private final val DEFAULT_PASSWORD = "postgres"
  private final val DEFAULT_CONNECTION_URL = s"jdbc:postgresql://$DEFAULT_HOST:$DEFAULT_PORT/" +
    s"$DEFAULT_DB?user=$DEFAULT_USER&password=$DEFAULT_PASSWORD"
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
          val row = Row.fromSeq((1 to metadata.getColumnCount).map(i => {
            val value = rs.getObject(i)
            if (value == null) { "NULL" } else { value }
          }))
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
    df.write.option("driver", POSTGRES_DRIVER_CLASS_NAME).mode("append")
      .jdbc(url, tableName, new Properties())
  }

  def close(): Unit = if (!conn.isClosed) conn.close()
}
