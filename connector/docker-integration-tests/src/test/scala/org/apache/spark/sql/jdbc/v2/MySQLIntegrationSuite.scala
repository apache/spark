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

import java.sql.{Connection, SQLFeatureNotSupportedException}

import org.apache.spark.{SparkConf, SparkSQLFeatureNotSupportedException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.MySQLDatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:9.0.1):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:9.0.1
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {

  override def excluded: Seq[String] = Seq(
    "scan with aggregate push-down: VAR_POP with DISTINCT",
    "scan with aggregate push-down: VAR_SAMP with DISTINCT",
    "scan with aggregate push-down: STDDEV_POP with DISTINCT",
    "scan with aggregate push-down: STDDEV_SAMP with DISTINCT",
    "scan with aggregate push-down: COVAR_POP with DISTINCT",
    "scan with aggregate push-down: COVAR_POP without DISTINCT",
    "scan with aggregate push-down: COVAR_SAMP with DISTINCT",
    "scan with aggregate push-down: COVAR_SAMP without DISTINCT",
    "scan with aggregate push-down: CORR with DISTINCT",
    "scan with aggregate push-down: CORR without DISTINCT",
    "scan with aggregate push-down: REGR_INTERCEPT with DISTINCT",
    "scan with aggregate push-down: REGR_INTERCEPT without DISTINCT",
    "scan with aggregate push-down: REGR_SLOPE with DISTINCT",
    "scan with aggregate push-down: REGR_SLOPE without DISTINCT",
    "scan with aggregate push-down: REGR_R2 with DISTINCT",
    "scan with aggregate push-down: REGR_R2 without DISTINCT",
    "scan with aggregate push-down: REGR_SXY with DISTINCT",
    "scan with aggregate push-down: REGR_SXY without DISTINCT")

  override val catalogName: String = "mysql"
  override val db = new MySQLDatabaseOnDocker

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mysql.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.mysql.pushDownAggregate", "true")
    .set("spark.sql.catalog.mysql.pushDownLimit", "true")
    .set("spark.sql.catalog.mysql.pushDownOffset", "true")

  private var mySQLVersion = -1

  override def tablePreparation(connection: Connection): Unit = {
    mySQLVersion = connection.getMetaData.getDatabaseMajorVersion
    connection.prepareStatement(
      "CREATE TABLE employee (dept INT, name VARCHAR(32), salary DECIMAL(20, 2)," +
        " bonus DOUBLE)").executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE pattern_testing_table (
         |pattern_testing_col LONGTEXT
         |)
         |""".stripMargin
    ).executeUpdate()
    connection.prepareStatement(
      "CREATE TABLE datetime (name VARCHAR(32), date1 DATE, time1 TIMESTAMP)")
      .executeUpdate()
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
      context = ExpectedContext(fragment = sql1, start = 0, stop = 55)
    )
  }

  override def testRenameColumn(tbl: String): Unit = {
    assert(mySQLVersion > 0)
    if (mySQLVersion < 8) {
      // Rename is unsupported for mysql versions < 8.0.
      val exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tbl RENAME COLUMN ID TO RENAMED")
      }
      assert(exception.getCause != null, s"Wrong exception thrown: $exception")
      val msg = exception.getCause.asInstanceOf[SQLFeatureNotSupportedException].getMessage
      assert(msg.contains("Rename column is only supported for MySQL version 8.0 and above."))
    } else {
      super.testRenameColumn(tbl)
    }
  }

  override def testUpdateColumnNullability(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID STRING NOT NULL)")
    // Update nullability is unsupported for mysql db.
    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        sql(s"ALTER TABLE $tbl ALTER COLUMN ID DROP NOT NULL")
      },
      condition = "UNSUPPORTED_FEATURE.UPDATE_COLUMN_NULLABILITY")
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('ENGINE'='InnoDB', 'DEFAULT CHARACTER SET'='utf8')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
  }

  override def supportsIndex: Boolean = true

  override def supportListIndexes: Boolean = true

  override def indexOptions: String = "KEY_BLOCK_SIZE=10"

  test("SPARK-42943: Use LONGTEXT instead of TEXT for StringType for effective length") {
    val tableName = catalogName + ".t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(c1 string)")
      sql(s"INSERT INTO $tableName SELECT rpad('hi', 65536, 'spark')")
      assert(sql(s"SELECT char_length(c1) from $tableName").head().get(0) === 65536)
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
      sql(s"CREATE TABLE mysql.new_table (i INT) TBLPROPERTIES('a'='1')")
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
    checkFilterPushed(df6)
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

    // MySQL does not support
    val df10 = sql(s"SELECT name FROM $tbl WHERE trunc(date1, 'week') = date'2022-05-16'")
    checkFilterPushed(df10, false)
    val rows10 = df10.collect()
    assert(rows10.length === 2)
    assert(rows10(0).getString(0) === "amy")
    assert(rows10(1).getString(0) === "alex")
  }
}

/**
 * To run this test suite for a specific version (e.g., mysql:9.0.1):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:9.0.1
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly *MySQLOverMariaConnectorIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLOverMariaConnectorIntegrationSuite extends MySQLIntegrationSuite {
  override def defaultMetadata(dataType: DataType = StringType): Metadata = new MetadataBuilder()
    .putLong("scale", 0)
    .putBoolean("isTimestampNTZ", false)
    .putBoolean("isSigned", true)
    .build()

  override val db = new MySQLDatabaseOnDocker {
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass&allowPublicKeyRetrieval=true" +
        s"&useSSL=false"
  }
}
