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
import java.util.Locale

import org.apache.spark.{SparkConf, SparkRuntimeException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.OracleDatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * The following are the steps to test this:
 *
 * 1. Choose to use a prebuilt image or build Oracle database in a container
 *    - The documentation on how to build Oracle RDBMS in a container is at
 *      https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
 *    - Official Oracle container images can be found at https://container-registry.oracle.com
 *    - Trustable and streamlined Oracle Database Free images can be found on Docker Hub at
 *      https://hub.docker.com/r/gvenzl/oracle-free
 *      see also https://github.com/gvenzl/oci-oracle-free
 * 2. Run: export ORACLE_DOCKER_IMAGE_NAME=image_you_want_to_use_for_testing
 *    - Example: export ORACLE_DOCKER_IMAGE_NAME=gvenzl/oracle-free:latest
 * 3. Run: export ENABLE_DOCKER_INTEGRATION_TESTS=1
 * 4. Start docker: sudo service docker start
 *    - Optionally, docker pull $ORACLE_DOCKER_IMAGE_NAME
 * 5. Run Spark integration tests for Oracle with: ./build/sbt -Pdocker-integration-tests
 *    "testOnly org.apache.spark.sql.jdbc.v2.OracleIntegrationSuite"
 *
 * A sequence of commands to build the Oracle Database Free container image:
 *  $ git clone https://github.com/oracle/docker-images.git
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles0
 *  $ ./buildContainerImage.sh -v 23.4.0 -f
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:23.4.0-free
 *
 * This procedure has been validated with Oracle Database Free version 23.4.0,
 * and with Oracle Express Edition versions 18.4.0 and 21.4.0
 */
@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {

  // Following tests are disabled for both single and multiple partition read
  override def excluded: Seq[String] = Seq(
    "scan with aggregate push-down: VAR_POP with DISTINCT (false)",
    "scan with aggregate push-down: VAR_POP with DISTINCT (true)",
    "scan with aggregate push-down: VAR_SAMP with DISTINCT (false)",
    "scan with aggregate push-down: VAR_SAMP with DISTINCT (true)",
    "scan with aggregate push-down: STDDEV_POP with DISTINCT (false)",
    "scan with aggregate push-down: STDDEV_POP with DISTINCT (true)",
    "scan with aggregate push-down: STDDEV_SAMP with DISTINCT (false)",
    "scan with aggregate push-down: STDDEV_SAMP with DISTINCT (true)",
    "scan with aggregate push-down: COVAR_POP with DISTINCT (false)",
    "scan with aggregate push-down: COVAR_POP with DISTINCT (true)",
    "scan with aggregate push-down: COVAR_SAMP with DISTINCT (false)",
    "scan with aggregate push-down: COVAR_SAMP with DISTINCT (true)",
    "scan with aggregate push-down: CORR with DISTINCT (false)",
    "scan with aggregate push-down: CORR with DISTINCT (true)",
    "scan with aggregate push-down: REGR_INTERCEPT with DISTINCT (false)",
    "scan with aggregate push-down: REGR_INTERCEPT with DISTINCT (true)",
    "scan with aggregate push-down: REGR_SLOPE with DISTINCT (false)",
    "scan with aggregate push-down: REGR_SLOPE with DISTINCT (true)",
    "scan with aggregate push-down: REGR_R2 with DISTINCT (false)",
    "scan with aggregate push-down: REGR_R2 with DISTINCT (true)",
    "scan with aggregate push-down: REGR_SXY with DISTINCT (false)",
    "scan with aggregate push-down: REGR_SXY with DISTINCT (true)")

  override val catalogName: String = "oracle"
  override val namespaceOpt: Option[String] = Some("SYSTEM")
  override val db = new OracleDatabaseOnDocker

  object JdbcClientTypes {
    val NUMBER = "NUMBER"
    val STRING = "VARCHAR2"
  }

  override def defaultMetadata(
      dataType: DataType = StringType,
      jdbcClientType: String = JdbcClientTypes.STRING): Metadata =
    new MetadataBuilder()
      .putLong("scale", 0)
      .putBoolean("isTimestampNTZ", false)
      .putBoolean(
        "isSigned",
        dataType.isInstanceOf[NumericType] || dataType.isInstanceOf[StringType])
      .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, "varchar(255)")
      .putString("jdbcClientType", jdbcClientType)
      .build()


  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.oracle", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.oracle.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.oracle.pushDownAggregate", "true")
    .set("spark.sql.catalog.oracle.pushDownLimit", "true")
    .set("spark.sql.catalog.oracle.pushDownOffset", "true")

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept NUMBER(32), name VARCHAR2(32), salary NUMBER(20, 2)," +
        " bonus BINARY_DOUBLE)").executeUpdate()
    connection.prepareStatement(
      """CREATE TABLE pattern_testing_table (
        |pattern_testing_col VARCHAR(50)
        |)
      """.stripMargin
    ).executeUpdate()
    connection.prepareStatement(
        "CREATE TABLE datetime (name VARCHAR(32), date1 DATE, time1 TIMESTAMP)")
      .executeUpdate()
  }

  override def dataPreparation(connection: Connection): Unit = {
    super.dataPreparation(connection)
    connection.prepareStatement(
      "INSERT INTO datetime VALUES ('amy', TO_DATE('2022-05-19', 'YYYY-MM-DD')," +
        " TO_TIMESTAMP('2022-05-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))").executeUpdate()
    connection.prepareStatement(
      "INSERT INTO datetime VALUES ('alex', TO_DATE('2022-05-18', 'YYYY-MM-DD')," +
        " TO_TIMESTAMP('2022-05-18 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))").executeUpdate()
    // '2022-01-01' is Saturday and is in ISO year 2021.
    connection.prepareStatement(
      "INSERT INTO datetime VALUES ('tom', TO_DATE('2022-01-01', 'YYYY-MM-DD')," +
        " TO_TIMESTAMP('2022-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))").executeUpdate()
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType()
      .add(
        "ID",
        DecimalType(10, 0),
        true,
        super.defaultMetadata(DecimalType(10, 0), JdbcClientTypes.NUMBER))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE LONG")
    t = spark.table(tbl)
    expectedSchema = new StructType()
      .add(
        "ID",
        DecimalType(19, 0),
        true,
        super.defaultMetadata(DecimalType(19, 0), JdbcClientTypes.NUMBER))
    assert(t.schema === expectedSchema)
    // Update column type from LONG to INTEGER
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      condition = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"DECIMAL(19,0)\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 56)
    )
  }

  override def caseConvert(tableName: String): String = tableName.toUpperCase(Locale.ROOT)

  test("SPARK-46478: Revert SPARK-43049 to use varchar(255) for string") {
    val tableName = catalogName + ".t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(c1 string)")
      checkError(
        exception = intercept[SparkRuntimeException] {
          sql(s"INSERT INTO $tableName SELECT rpad('hi', 256, 'spark')")
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "255")
      )
    }
  }

  test("SPARK-47879: Use VARCHAR2 instead of VARCHAR") {
    val tableName = catalogName + ".t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(c1 varchar(10), c2 char(3))")
      sql(s"INSERT INTO $tableName SELECT 'Eason' as c1, 'Y' as c2")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row("Eason", "Y  ")))
    }
  }

  override def testDatetime(tbl: String): Unit = {
    val df1 = sql(s"SELECT name FROM $tbl WHERE " +
      "dayofyear(date1) > 100 AND dayofmonth(date1) > 10 ")
    checkFilterPushed(df1, false)
    val rows1 = df1.collect()
    assert(rows1.length === 2)
    assert(rows1(0).getString(0) === "amy")
    assert(rows1(1).getString(0) === "alex")

    val df2 = sql(s"SELECT name FROM $tbl WHERE year(date1) = 2022 AND quarter(date1) = 2")
    checkFilterPushed(df2, false)
    val rows2 = df2.collect()
    assert(rows2.length === 2)
    assert(rows2(0).getString(0) === "amy")
    assert(rows2(1).getString(0) === "alex")

    val df3 = sql(s"SELECT name FROM $tbl WHERE month(date1) = 5")
    checkFilterPushed(df3)
    val rows3 = df3.collect()
    assert(rows3.length === 2)
    assert(rows3(0).getString(0) === "amy")
    assert(rows3(1).getString(0) === "alex")

    val df4 = sql(s"SELECT name FROM $tbl WHERE hour(time1) = 0 AND minute(time1) = 0")
    checkFilterPushed(df4)
    val rows4 = df4.collect()
    assert(rows4.length === 3)
    assert(rows4(0).getString(0) === "amy")
    assert(rows4(1).getString(0) === "alex")
    assert(rows4(2).getString(0) === "tom")

    val df5 = sql(s"SELECT name FROM $tbl WHERE " +
      "extract(WEEK from date1) > 10 AND extract(YEAR from date1) = 2022")
    checkFilterPushed(df5, false)
    val rows5 = df5.collect()
    assert(rows5.length === 3)
    assert(rows5(0).getString(0) === "amy")
    assert(rows5(1).getString(0) === "alex")
    assert(rows5(2).getString(0) === "tom")

    val df6 = sql(s"SELECT name FROM $tbl WHERE date_add(date1, 1) = date'2022-05-20' " +
      "AND datediff(date1, '2022-05-10') > 0")
    checkFilterPushed(df6, false)
    val rows6 = df6.collect()
    assert(rows6.length === 1)
    assert(rows6(0).getString(0) === "amy")

    val df7 = sql(s"SELECT name FROM $tbl WHERE weekday(date1) = 2")
    checkFilterPushed(df7, false)
    val rows7 = df7.collect()
    assert(rows7.length === 1)
    assert(rows7(0).getString(0) === "alex")

    withClue("weekofyear") {
      val woy = sql(s"SELECT weekofyear(date1) FROM $tbl WHERE name = 'tom'")
        .collect().head.getInt(0)
      val df = sql(s"SELECT name FROM $tbl WHERE weekofyear(date1) = $woy")
      checkFilterPushed(df, false)
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows(0).getString(0) === "tom")
    }

    withClue("dayofweek") {
      val dow = sql(s"SELECT dayofweek(date1) FROM $tbl WHERE name = 'alex'")
        .collect().head.getInt(0)
      val df = sql(s"SELECT name FROM $tbl WHERE dayofweek(date1) = $dow")
      checkFilterPushed(df, false)
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows(0).getString(0) === "alex")
    }

    withClue("yearofweek") {
      val yow = sql(s"SELECT extract(YEAROFWEEK from date1) FROM $tbl WHERE name = 'tom'")
        .collect().head.getInt(0)
      val df = sql(s"SELECT name FROM $tbl WHERE extract(YEAROFWEEK from date1) = $yow")
      checkFilterPushed(df, false)
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows(0).getString(0) === "tom")
    }

    withClue("dayofyear") {
      val doy = sql(s"SELECT dayofyear(date1) FROM $tbl WHERE name = 'amy'")
        .collect().head.getInt(0)
      val df = sql(s"SELECT name FROM $tbl WHERE dayofyear(date1) = $doy")
      checkFilterPushed(df, false)
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows(0).getString(0) === "amy")
    }

    withClue("dayofmonth") {
      val dom = sql(s"SELECT dayofmonth(date1) FROM $tbl WHERE name = 'amy'")
        .collect().head.getInt(0)
      val df = sql(s"SELECT name FROM $tbl WHERE dayofmonth(date1) = $dom")
      checkFilterPushed(df)
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows(0).getString(0) === "amy")
    }

    withClue("year") {
      val year = sql(s"SELECT year(date1) FROM $tbl WHERE name = 'amy'")
        .collect().head.getInt(0)
      val df = sql(s"SELECT name FROM $tbl WHERE year(date1) = $year")
      checkFilterPushed(df)
      val rows = df.collect()
      assert(rows.length === 3)
      assert(rows(0).getString(0) === "amy")
      assert(rows5(1).getString(0) === "alex")
      assert(rows5(2).getString(0) === "tom")
    }

    withClue("second") {
      val df = sql(s"SELECT name FROM $tbl WHERE second(time1) = 0 AND month(date1) = 5")
      checkFilterPushed(df, false)
      val rows = df.collect()
      assert(rows.length === 2)
      assert(rows(0).getString(0) === "amy")
      assert(rows(1).getString(0) === "alex")
    }

    val df9 = sql(s"SELECT name FROM $tbl WHERE " +
      "dayofyear(date1) > 100 order by dayofyear(date1) limit 1")
    checkFilterPushed(df9, false)
    val rows9 = df9.collect()
    assert(rows9.length === 1)
    assert(rows9(0).getString(0) === "alex")

    val df10 = sql(s"SELECT name FROM $tbl WHERE trunc(date1, 'week') = date'2022-05-16'")
    checkFilterPushed(df10)
    val rows10 = df10.collect()
    assert(rows10.length === 2)
    assert(rows10(0).getString(0) === "amy")
    assert(rows10(1).getString(0) === "alex")
  }
}
