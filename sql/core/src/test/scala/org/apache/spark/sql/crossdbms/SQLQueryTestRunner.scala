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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.test.SQLTestUtils

trait SQLQueryTestRunner {

  /**
   * Runs a given query.
   * @return Returns a tuple with first argument being the schema and the second argument being a
   *         Sequence of strings representing the output.
   */
  def runQuery(query: String): (String, Seq[String])

  /**
   * Perform clean up, such as dropping tables and closing the database connection.
   */
  def cleanUp(): Unit
}

/**
 * A runner that takes a JDBC connection and uses it to execute queries. We still need the local
 * Spark session to do some things, such as generate the schema and load test data into
 * dataframes, before creating tables in the database.
 */
private[sql] case class JdbcSQLQueryTestRunner(
  connection: JdbcConnection, spark: SparkSession) extends SQLQueryTestRunner with SQLTestUtils {
  setUp()

  def runQuery(query: String): (String, Seq[String]) = {
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeCommandBase
           | _: DescribeColumnCommand
           | _: DescribeRelation
           | _: DescribeColumn => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true

      case _ => plan.children.iterator.exists(isSorted)
    }

    val df = spark.sql(query)
    val schema = df.schema.catalogString
    val output = connection.runQuery(query)
    // Use Spark analyzed plan to check if the query result is already semantically sorted.
    val result = if (isSorted(df.queryExecution.analyzed)) {
      output
    } else {
      // Sort the answer manually if it isn't sorted.
      output.sorted
    }
    // Use the Spark schema for schema generation.
    (schema, result)
  }

  def cleanUp(): Unit = {
    connection.close()
  }

  private def setUp(): Unit = {
    loadTestData()
  }

  override def loadTestData(): Unit = {} // TODO: Load test data
}