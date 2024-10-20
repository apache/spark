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

import org.apache.spark.{SparkConf, SparkSQLException, SparkRuntimeException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.DatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., postgres:17.0-alpine)
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:17.0-alpine
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.PostgresIntegrationSuite"
 * }}}
 */
@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {
  override val catalogName: String = "postgresql"
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:17.0-alpine")
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
    .set("spark.sql.catalog.postgresql.pushDownAggregate", "true")
    .set("spark.sql.catalog.postgresql.pushDownOffset", "true")

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept INTEGER, name VARCHAR(32), salary NUMERIC(20, 2)," +
        " bonus double precision)").executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE pattern_testing_table (
         |pattern_testing_col VARCHAR(50)
         |)
                   """.stripMargin
    ).executeUpdate()

    connection.prepareStatement("CREATE TABLE array_test_table (int_array int[]," +
      "float_array FLOAT8[], timestamp_array TIMESTAMP[], string_array TEXT[]," +
      "datetime_array TIMESTAMPTZ[], array_of_int_arrays INT[][])").executeUpdate()

    val query =
      """
        INSERT INTO array_test_table
        (int_array, float_array, timestamp_array, string_array,
        datetime_array, array_of_int_arrays)
        VALUES
        (
            ARRAY[1, 2, 3],                       -- Array of integers
            ARRAY[1.1, 2.2, 3.3],                 -- Array of floats
            ARRAY['2023-01-01 12:00'::timestamp, '2023-06-01 08:30'::timestamp],
            ARRAY['hello', 'world'],              -- Array of strings
            ARRAY['2023-10-04 12:00:00+00'::timestamptz,
            '2023-12-01 14:15:00+00'::timestamptz],
            ARRAY[ARRAY[1, 2]]    -- Array of arrays of integers
        ),
        (
            ARRAY[10, 20, 30],                    -- Another set of data
            ARRAY[10.5, 20.5, 30.5],
            ARRAY['2022-01-01 09:15'::timestamp, '2022-03-15 07:45'::timestamp],
            ARRAY['postgres', 'arrays'],
            ARRAY['2022-11-22 09:00:00+00'::timestamptz,
            '2022-12-31 23:59:59+00'::timestamptz],
            ARRAY[ARRAY[10, 20]]
        );
      """
    connection.prepareStatement(query).executeUpdate()

    connection.prepareStatement("CREATE TABLE array_int (col int[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_bigint(col bigint[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_smallint (col smallint[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_boolean (col boolean[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_float (col real[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_double (col float8[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_timestamp (col timestamp[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_timestamptz (col timestamptz[])")
      .executeUpdate()

    connection.prepareStatement("INSERT INTO array_int VALUES (array[array[10]])").executeUpdate()
    connection.prepareStatement("INSERT INTO array_bigint VALUES (array[array[10]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_smallint VALUES (array[array[10]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_boolean VALUES (array[array[true]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_float VALUES (array[array[10.5]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_double VALUES (array[array[10.1]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_timestamp VALUES (" +
      "array[array['2022-01-01 09:15'::timestamp]])").executeUpdate()
    connection.prepareStatement("INSERT INTO array_timestamptz VALUES " +
      "(array[array['2022-01-01 09:15'::timestamptz]])").executeUpdate()
    connection.prepareStatement(
    "CREATE TABLE datetime (name VARCHAR(32), date1 DATE, time1 TIMESTAMP)")
    .executeUpdate()
  }

  test("Test multi-dimensional column types") {
    // This test is used to verify that the multi-dimensional
    // column types are supported by the JDBC V2 data source.
    // We do not verify any result output
    //
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "array_test_table")
      .load()
    df.collect()

    val array_tables = Array(
      ("array_int", "\"ARRAY<INT>\""),
      ("array_bigint", "\"ARRAY<BIGINT>\""),
      ("array_smallint", "\"ARRAY<SMALLINT>\""),
      ("array_boolean", "\"ARRAY<BOOLEAN>\""),
      ("array_float", "\"ARRAY<FLOAT>\""),
      ("array_double", "\"ARRAY<DOUBLE>\""),
      ("array_timestamp", "\"ARRAY<TIMESTAMP>\""),
      ("array_timestamptz", "\"ARRAY<TIMESTAMP>\"")
    )

    array_tables.foreach { case (dbtable, arrayType) =>
      checkError(
        exception = intercept[SparkSQLException] {
          val df = spark.read.format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", dbtable)
            .load()
          df.collect()
        },
        condition = "COLUMN_ARRAY_ELEMENT_TYPE_MISMATCH",
        parameters = Map("pos" -> "0", "type" -> arrayType),
        sqlState = Some("0A000")
      )
    }
  }

  override def dataPreparation(connection: Connection): Unit = {
    super.dataPreparation(connection)
    connection.prepareStatement("INSERT INTO datetime VALUES " +
      "('amy', '2022-05-19', '2022-05-19 00:00:00')").executeUpdate()
    connection.prepareStatement("INSERT INTO datetime VALUES " +
      "('alex', '2022-05-18', '2022-05-18 00:00:00')").executeUpdate()
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType()
      .add("ID", StringType, true, defaultMetadata())
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      condition = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"STRING\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 60)
    )
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('TABLESPACE'='pg_default')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
  }

  override def supportsTableSample: Boolean = true

  override def supportsIndex: Boolean = true

  override def indexOptions: String = "FILLFACTOR=70"

  test("SPARK-42964: SQLState: 42P07 - duplicated table") {
    val t1 = s"$catalogName.t1"
    val t2 = s"$catalogName.t2"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1(c int)")
      sql(s"CREATE TABLE $t2(c int)")
      checkError(
        exception = intercept[TableAlreadyExistsException](sql(s"ALTER TABLE $t1 RENAME TO t2")),
        condition = "TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> "`t2`")
      )
    }
  }

  test("SPARK-49730: syntax error classification") {
    checkError(
      exception = intercept[AnalysisException] {
        val schema = StructType(
          Seq(StructField("id", IntegerType, true)))

        spark.read
          .format("jdbc")
          .schema(schema)
          .option("url", jdbcUrl)
          .option("query", "SELECT * FRM range(10)")
          .load()
      },
      condition = "FAILED_JDBC.SYNTAX_ERROR",
      parameters = Map(
        "url" -> jdbcUrl,
        "query" -> "SELECT * FROM (SELECT * FRM range(10)) SPARK_GEN_SUBQ_0 WHERE 1=0"))
  }

  test("SPARK-49730: get_schema error classification") {
    checkError(
      exception = intercept[AnalysisException] {
        val schema = StructType(
          Seq(StructField("id", IntegerType, true)))

        spark.read
          .format("jdbc")
          .schema(schema)
          .option("url", jdbcUrl)
          .option("query", "SELECT * FROM non_existent_table")
          .load()
      },
      condition = "FAILED_JDBC.GET_SCHEMA",
      parameters = Map(
        "url" -> jdbcUrl,
        "query" -> "SELECT * FROM (SELECT * FROM non_existent_table) SPARK_GEN_SUBQ_8 WHERE 1=0"))
  }

  test("SPARK-49730: create_table error classification") {
    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE postgresql.new_table (i INT) TBLPROPERTIES('a'='1')")
    }

    checkErrorMatchPVals(
      exception = e,
      condition = "FAILED_JDBC.CREATE_TABLE",
      parameters = Map(
        "url" -> "jdbc:.*",
        "tableName" -> s"`new_table`"
      )
    )
  }

  override def testDatetime(tbl: String): Unit = {
    val df1 = sql(s"SELECT name FROM $tbl WHERE " +
      "dayofyear(date1) > 100 AND dayofmonth(date1) > 10 ")
    checkFilterPushed(df1)
    val rows1 = df1.collect()
    assert(rows1.length === 2)
    assert(rows1(0).getString(0) === "amy")
    assert(rows1(1).getString(0) === "alex")

    val df2 = sql(s"SELECT name FROM $tbl WHERE year(date1) = 2022 AND quarter(date1) = 2")
    checkFilterPushed(df2)
    val rows2 = df2.collect()
    assert(rows2.length === 2)
    assert(rows2(0).getString(0) === "amy")
    assert(rows2(1).getString(0) === "alex")

    val df3 = sql(s"SELECT name FROM $tbl WHERE second(time1) = 0 AND month(date1) = 5")
    checkFilterPushed(df3)
    val rows3 = df3.collect()
    assert(rows3.length === 2)
    assert(rows3(0).getString(0) === "amy")
    assert(rows3(1).getString(0) === "alex")

    val df4 = sql(s"SELECT name FROM $tbl WHERE hour(time1) = 0 AND minute(time1) = 0")
    checkFilterPushed(df4)
    val rows4 = df4.collect()
    assert(rows4.length === 2)
    assert(rows4(0).getString(0) === "amy")
    assert(rows4(1).getString(0) === "alex")

    val df5 = sql(s"SELECT name FROM $tbl WHERE " +
      "extract(WEEk from date1) > 10 AND extract(YEAROFWEEK from date1) = 2022")
    checkFilterPushed(df5)
    val rows5 = df5.collect()
    assert(rows5.length === 2)
    assert(rows5(0).getString(0) === "amy")
    assert(rows5(1).getString(0) === "alex")

    val df6 = sql(s"SELECT name FROM $tbl WHERE date_add(date1, 1) = date'2022-05-20' " +
      "AND datediff(date1, '2022-05-10') > 0")
    checkFilterPushed(df6, false)
    val rows6 = df6.collect()
    assert(rows6.length === 1)
    assert(rows6(0).getString(0) === "amy")

    val df7 = sql(s"SELECT name FROM $tbl WHERE weekday(date1) = 2")
    checkFilterPushed(df7)
    val rows7 = df7.collect()
    assert(rows7.length === 1)
    assert(rows7(0).getString(0) === "alex")

    val df8 = sql(s"SELECT name FROM $tbl WHERE dayofweek(date1) = 4")
    checkFilterPushed(df8)
    val rows8 = df8.collect()
    assert(rows8.length === 1)
    assert(rows8(0).getString(0) === "alex")

    val df9 = sql(s"SELECT name FROM $tbl WHERE " +
      "dayofyear(date1) > 100 order by dayofyear(date1) limit 1")
    checkFilterPushed(df9)
    val rows9 = df9.collect()
    assert(rows9.length === 1)
    assert(rows9(0).getString(0) === "alex")

    // Postgres does not support
    val df10 = sql(s"SELECT name FROM $tbl WHERE trunc(date1, 'week') = date'2022-05-16'")
    checkFilterPushed(df10, false)
    val rows10 = df10.collect()
    assert(rows10.length === 2)
    assert(rows10(0).getString(0) === "amy")
    assert(rows10(1).getString(0) === "alex")
  }
}
