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

package jdbcv2

import java.sql.DriverManager
import java.util.Properties

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.jdbc.jdbcv2.JDBCDataSourceReader
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec
import org.apache.spark.sql.test.SharedSQLContext

class JDBCDataSourceV2Suite extends QueryTest
  with BeforeAndAfter with SharedSQLContext {

  import testImplicits._

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  before {
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement(
      "insert into test.people values ('joe ''foo'' \"bar\"', 3)").executeUpdate()
    conn.commit()
  }

  after {
    conn.close()
  }

  def getReader(query: DataFrame): JDBCDataSourceReader = {
    query.queryExecution.executedPlan.collect {
      case d: DataSourceV2ScanExec => d.reader.asInstanceOf[JDBCDataSourceReader]
    }.head
  }

  test("JDBCDataSourceV2 Implementation") {
    val df = spark.read.format("jdbcv2")
      .option("url", urlWithUserAndPass)
      .option("dbtable", "TEST.PEOPLE")
      .option("partitionColumn", "THEID")
      .option("lowerBound", 0)
      .option("upperBound", 3)
      .option("numPartitions", 3)
      .load()

    val expectedDF = Seq(("fred", 1), ("mary", 2), ("joe 'foo' \"bar\"", 3)).toDF("NAME", "THEID")
    checkAnswer(df, expectedDF)
    assert(getReader(df).planInputPartitions().size === 3)

    val df2 = df.select("NAME").filter("THEID = 1")
    val expectedDF2 = Seq(("fred")).toDF("NAME")
    checkAnswer(df2, expectedDF2)

    val reader = getReader(df2)
    assert(reader.pushedFilters().flatMap(_.references).toSet === Set("THEID"))
    assert(reader.pushedFiltersArray.flatMap(_.references).toSet === Set("THEID"))
    assert(reader.requiredSchema.fieldNames === Seq("NAME"))
  }
}
