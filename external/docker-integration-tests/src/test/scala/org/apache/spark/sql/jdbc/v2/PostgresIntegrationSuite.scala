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

package org.apache.spark.sql.jdbc.v2

import java.sql.Connection

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite}
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., postgres:14.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:14.0
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.PostgresIntegrationSuite"
 * }}}
 */
@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationSuite with V2JDBCTest {
  override val catalogName: String = "postgresql"
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:14.0-alpine")
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
  }
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.postgresql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.postgresql.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.postgresql.pushDownTableSample", "true")
    .set("spark.sql.catalog.postgresql.pushDownLimit", "true")

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE SCHEMA \"test\"").executeUpdate()
    conn.prepareStatement("CREATE TYPE position_type AS (x NUMERIC(20, 2), y NUMERIC(20, 2))")
      .executeUpdate()
    conn.prepareStatement(
      "CREATE TABLE \"test\".\"employee\" (dept INTEGER, name VARCHAR(32), salary NUMERIC(20, 2)," +
        " bonus double precision, position position_type)")
      .executeUpdate()
    conn.prepareStatement(
      "INSERT INTO \"test\".\"employee\" VALUES (1, 'amy', 10000, 1000, '(1, 2)')").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO \"test\".\"employee\" VALUES (2, 'alex', 12000, 1200, '(1, 2)')").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO \"test\".\"employee\" VALUES (1, 'cathy', 9000, 1200, '(2, 3)')").executeUpdate()
    conn.prepareStatement(
"INSERT INTO \"test\".\"employee\" VALUES (2, 'david', 10000, 1300, '(2, 4)')")
      .executeUpdate()
    conn.prepareStatement(
      "INSERT INTO \"test\".\"employee\" VALUES (6, 'jen', 12000, 1200, '(3, 4))").executeUpdate()
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER")
    }.getMessage
    assert(msg.contains(
      s"Cannot update $catalogName.alt_table field ID: string cannot be cast to int"))
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('TABLESPACE'='pg_default')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
  }

  override def supportsTableSample: Boolean = true

  override def supportsIndex: Boolean = true

  override def indexOptions: String = "FILLFACTOR=70"

  test("scan with aggregate push-down: group by with nested column") {
    val df = sql("select position.x, MAX(employee.SALARY) FROM postgresql.test.employee" +
      " where dept > 0 group by position.x")
    checkAggregateRemoved(df)
    val row = df.collect()
    assert(row.length === 3)
    assert(row(0).length === 2)
    assert(row(0).getDouble(0) === 10000d)
    assert(row(0).getDouble(1) === 20000d)
    assert(row(1).getDouble(0) === 2500d)
    assert(row(1).getDouble(1) === 5000d)
    assert(row(2).getDouble(0) === 0d)
    assert(row(2).isNullAt(1))
  }

  private def checkAggregateRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val aggregates = df.queryExecution.optimizedPlan.collect {
      case agg: Aggregate => agg
    }
    if (removed) {
      assert(aggregates.isEmpty)
    } else {
      assert(aggregates.nonEmpty)
    }
  }
}
