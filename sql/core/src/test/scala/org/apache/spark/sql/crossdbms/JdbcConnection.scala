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

private[sql] trait JdbcConnection {
  /**
   * Runs the given query.
   * @return A Seq[String] representing the output.
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

var runner: Option[SQLQueryTestRunner] = None

  // Run the SQL queries preparing them for comparison.
  val outputs: Seq[QueryTestOutput] = queries.map { sql =>
  testCase match {
  case _: AnalyzerTest =>
  val (_, output) =
  handleExceptions(getNormalizedQueryAnalysisResult(localSparkSession, sql))
  // We do some query canonicalization now.
  AnalyzerOutput(
  sql = sql,
  schema = None,
  output = normalizeTestResults(output.mkString("\n")))
  case _ =>
  val (schema, output) =
  if (regenerateGoldenFiles && crossDbmsToGenerateGoldenFiles.nonEmpty) {
  if (runner.isEmpty) {
  val connectionUrl = if (customConnectionUrl.nonEmpty) {
  Some(customConnectionUrl)
  } else {
  None
  }
  runner = Some(DBMS_MAPPING(crossDbmsToGenerateGoldenFiles)(connectionUrl))
  }
  val output = handleExceptions(runner.map(_.runQuery(sql)).get)
  val schema = spark.sql(sql).schema.catalogString
  (schema, output)
  } else {
  handleExceptions(getNormalizedQueryExecutionResult(localSparkSession, sql))
  }
  // We do some query canonicalization now.
  val executionOutput = ExecutionOutput(
  sql = sql,
  schema = Some(schema),
  output = normalizeTestResults(output.mkString("\n")))
  if (testCase.isInstanceOf[CTETest]) {
  expandCTEQueryAndCompareResult(localSparkSession, sql, executionOutput)
  }
  executionOutput
  }
  }
  jdbcRunner.foreach(_.cleanUp())