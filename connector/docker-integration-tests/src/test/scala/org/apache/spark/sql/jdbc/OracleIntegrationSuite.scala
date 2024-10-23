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

package org.apache.spark.sql.jdbc

import java.math.BigDecimal
import java.sql.{Connection, Date, Timestamp}
import java.util.{Properties, TimeZone}

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkSQLException
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.execution.{RowDataSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * The following are the steps to test this:
 *
 * 1. Choose to use a prebuilt image or build Oracle database in a container
 *    - The documentation on how to build Oracle RDBMS in a container is at
 *      https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
 *    - Official Oracle container images can be found at https://container-registry.oracle.com
 *    - A trustable and streamlined Oracle XE database image can be found on Docker Hub at
 *      https://hub.docker.com/r/gvenzl/oracle-xe see also https://github.com/gvenzl/oci-oracle-xe
 * 2. Run: export ORACLE_DOCKER_IMAGE_NAME=image_you_want_to_use_for_testing
 *    - Example: export ORACLE_DOCKER_IMAGE_NAME=gvenzl/oracle-xe:latest
 * 3. Run: export ENABLE_DOCKER_INTEGRATION_TESTS=1
 * 4. Start docker: sudo service docker start
 *    - Optionally, docker pull $ORACLE_DOCKER_IMAGE_NAME
 * 5. Run Spark integration tests for Oracle with: ./build/sbt -Pdocker-integration-tests
 *    "docker-integration-tests/testOnly org.apache.spark.sql.jdbc.OracleIntegrationSuite"
 *
 * A sequence of commands to build the Oracle XE database container image:
 *  $ git clone https://github.com/oracle/docker-images.git
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles
 *  $ ./buildContainerImage.sh -v 21.3.0 -x
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:21.3.0-xe
 *
 * This procedure has been validated with Oracle 18.4.0 and 21.3.0 Express Edition.
 */
@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSparkSession {
  import testImplicits._

  override val db = new DatabaseOnDocker {
    lazy override val imageName =
      sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "gvenzl/oracle-xe:21.3.0")
    val oracle_password = "Th1s1sThe0racle#Pass"
    override val env = Map(
      "ORACLE_PWD" -> oracle_password,      // oracle images uses this
      "ORACLE_PASSWORD" -> oracle_password  // gvenzl/oracle-xe uses this
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/$oracle_password@//$ip:$port/xe"
  }

  override val connectionTimeout = timeout(7.minutes)

  override def dataPreparation(conn: Connection): Unit = {
    // In 18.4.0 Express Edition auto commit is enabled by default.
    conn.setAutoCommit(false)
    conn.prepareStatement("CREATE TABLE datetime (id NUMBER(10), d DATE, t TIMESTAMP)")
      .executeUpdate()
    conn.prepareStatement(
      """INSERT INTO datetime VALUES
        |(1, {d '1991-11-09'}, {ts '1996-01-01 01:23:45'})
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.commit()

    conn.prepareStatement(
      "CREATE TABLE ts_with_timezone (id NUMBER(10), t TIMESTAMP WITH TIME ZONE)").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO ts_with_timezone VALUES " +
        "(1, to_timestamp_tz('1999-12-01 11:00:00 UTC','YYYY-MM-DD HH:MI:SS TZR'))").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO ts_with_timezone VALUES " +
        "(2, to_timestamp_tz('1999-12-01 12:00:00 PST','YYYY-MM-DD HH:MI:SS TZR'))").executeUpdate()
    conn.commit()

    conn.prepareStatement(
      "CREATE TABLE tableWithCustomSchema (id NUMBER, n1 NUMBER(1), n2 NUMBER(1))").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO tableWithCustomSchema values(12312321321321312312312312123, 1, 0)")
      .executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE TEMPORARY VIEW datetime
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$jdbcUrl', dbTable 'datetime', oracle.jdbc.mapDateToTimestamp 'false')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("CREATE TABLE datetime1 (id NUMBER(10), d DATE, t TIMESTAMP)")
      .executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE TEMPORARY VIEW datetime1
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$jdbcUrl', dbTable 'datetime1', oracle.jdbc.mapDateToTimestamp 'false')
      """.stripMargin.replaceAll("\n", " "))


    conn.prepareStatement("CREATE TABLE numerics (b DECIMAL(1), f DECIMAL(3, 2), i DECIMAL(10))")
      .executeUpdate()
    conn.prepareStatement(
      "INSERT INTO numerics VALUES (4, 1.23, 9999999999)").executeUpdate()
    conn.commit()

    conn.prepareStatement("CREATE TABLE oracle_types (d BINARY_DOUBLE, f BINARY_FLOAT)")
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("CREATE TABLE datetimePartitionTest (id NUMBER(10), d DATE, t TIMESTAMP)")
      .executeUpdate()
    conn.prepareStatement(
      """INSERT INTO datetimePartitionTest VALUES
        |(1, {d '2018-07-06'}, {ts '2018-07-06 05:50:00'})
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.prepareStatement(
      """INSERT INTO datetimePartitionTest VALUES
        |(2, {d '2018-07-06'}, {ts '2018-07-06 08:10:08'})
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.prepareStatement(
      """INSERT INTO datetimePartitionTest VALUES
        |(3, {d '2018-07-08'}, {ts '2018-07-08 13:32:01'})
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.prepareStatement(
      """INSERT INTO datetimePartitionTest VALUES
        |(4, {d '2018-07-12'}, {ts '2018-07-12 09:51:15'})
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.commit()
  }

  test("SPARK-16625 : Importing Oracle numeric types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "numerics", new Properties)
    val rows = df.collect()
    assert(rows.size == 1)
    val row = rows(0)
    // The main point of the below assertions is not to make sure that these Oracle types are
    // mapped to decimal types, but to make sure that the returned values are correct.
    // A value > 1 from DECIMAL(1) is correct:
    assert(row.getDecimal(0).compareTo(BigDecimal.valueOf(4)) == 0)
    // A value with fractions from DECIMAL(3, 2) is correct:
    assert(row.getDecimal(1).compareTo(BigDecimal.valueOf(1.23)) == 0)
    // A value > Int.MaxValue from DECIMAL(10) is correct:
    assert(row.getDecimal(2).compareTo(BigDecimal.valueOf(9999999999L)) == 0)
  }


  test("SPARK-12941: String datatypes to be mapped to VARCHAR(255) in Oracle") {
    // create a sample dataframe with string type
    val df1 = sparkContext.parallelize(Seq(("foo"))).toDF("x")
    // write the dataframe to the oracle table tbl
    df1.write.jdbc(jdbcUrl, "tbl2", new Properties)
    // read the table from the oracle
    val dfRead = sqlContext.read.jdbc(jdbcUrl, "tbl2", new Properties)
    // get the rows
    val rows = dfRead.collect()
    // verify the data type is inserted
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(0).equals("class java.lang.String"))
    // verify the value is the inserted correct or not
    assert(rows(0).getString(0).equals("foo"))
  }

  test("SPARK-16625: General data types to be mapped to Oracle") {
    val props = new Properties()
    props.put("oracle.jdbc.mapDateToTimestamp", "false")

    val schema = StructType(Seq(
      StructField("boolean_type", BooleanType, true),
      StructField("integer_type", IntegerType, true),
      StructField("long_type", LongType, true),
      StructField("float_Type", FloatType, true),
      StructField("double_type", DoubleType, true),
      StructField("byte_type", ByteType, true),
      StructField("short_type", ShortType, true),
      StructField("string_type", StringType, true),
      StructField("binary_type", BinaryType, true),
      StructField("date_type", DateType, true),
      StructField("timestamp_type", TimestampType, true)
    ))

    val tableName = "test_oracle_general_types"
    val booleanVal = true
    val integerVal = 1
    val longVal = 2L
    val floatVal = 3.0f
    val doubleVal = 4.0
    val byteVal = 2.toByte
    val shortVal = 5.toShort
    val stringVal = "string"
    val binaryVal = Array[Byte](6, 7, 8)
    val dateVal = Date.valueOf("2016-07-26")
    val timestampVal = Timestamp.valueOf("2016-07-26 11:49:45")

    val data = spark.sparkContext.parallelize(Seq(
      Row(
        booleanVal, integerVal, longVal, floatVal, doubleVal, byteVal, shortVal, stringVal,
        binaryVal, dateVal, timestampVal
      )))

    val dfWrite = spark.createDataFrame(data, schema)
    dfWrite.write.jdbc(jdbcUrl, tableName, props)

    val dfRead = spark.read.jdbc(jdbcUrl, tableName, props)
    val rows = dfRead.collect()
    // verify the data type is inserted
    val types = dfRead.schema.map(field => field.dataType)
    assert(types(0).equals(DecimalType(1, 0)))
    assert(types(1).equals(DecimalType(10, 0)))
    assert(types(2).equals(DecimalType(19, 0)))
    assert(types(3).equals(DecimalType(19, 4)))
    assert(types(4).equals(DecimalType(19, 4)))
    assert(types(5).equals(DecimalType(3, 0)))
    assert(types(6).equals(DecimalType(5, 0)))
    assert(types(7).equals(StringType))
    assert(types(8).equals(BinaryType))
    assert(types(9).equals(DateType))
    assert(types(10).equals(TimestampType))

    // verify the value is the inserted correct or not
    val values = rows(0)
    assert(values.getDecimal(0).compareTo(BigDecimal.valueOf(1)) == 0)
    assert(values.getDecimal(1).compareTo(BigDecimal.valueOf(integerVal)) == 0)
    assert(values.getDecimal(2).compareTo(BigDecimal.valueOf(longVal)) == 0)
    assert(values.getDecimal(3).compareTo(BigDecimal.valueOf(floatVal)) == 0)
    assert(values.getDecimal(4).compareTo(BigDecimal.valueOf(doubleVal)) == 0)
    assert(values.getDecimal(5).compareTo(BigDecimal.valueOf(byteVal)) == 0)
    assert(values.getDecimal(6).compareTo(BigDecimal.valueOf(shortVal)) == 0)
    assert(values.getString(7).equals(stringVal))
    assert(values.getAs[Array[Byte]](8).mkString.equals("678"))
    assert(values.getDate(9).equals(dateVal))
    assert(values.getTimestamp(10).equals(timestampVal))
  }

  test("SPARK-19318: connection property keys should be case-sensitive") {
    def checkRow(row: Row): Unit = {
      assert(row.getDecimal(0).equals(BigDecimal.valueOf(1)))
      assert(row.getDate(1).equals(Date.valueOf("1991-11-09")))
      assert(row.getTimestamp(2).equals(Timestamp.valueOf("1996-01-01 01:23:45")))
    }
    checkRow(sql("SELECT * FROM datetime where id = 1").head())
    sql("INSERT INTO TABLE datetime1 SELECT * FROM datetime where id = 1")
    checkRow(sql("SELECT * FROM datetime1 where id = 1").head())
  }

  test("SPARK-20557: column type TIMESTAMP with TIME ZONE should be recognized") {
    val dfRead = sqlContext.read.jdbc(jdbcUrl, "ts_with_timezone", new Properties)
    val rows = dfRead.collect()
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(1).equals("class java.sql.Timestamp"))
  }

  test("Column type TIMESTAMP with SESSION_LOCAL_TIMEZONE is different from default") {
    val defaultJVMTimeZone = TimeZone.getDefault
    // Pick the timezone different from the current default time zone of JVM
    val sofiaTimeZone = TimeZone.getTimeZone("Europe/Sofia")
    val shanghaiTimeZone = TimeZone.getTimeZone("Asia/Shanghai")
    val localSessionTimeZone =
      if (defaultJVMTimeZone == shanghaiTimeZone) sofiaTimeZone else shanghaiTimeZone

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> localSessionTimeZone.getID) {
      checkError(
        exception = intercept[SparkSQLException] {
          sqlContext.read.jdbc(jdbcUrl, "ts_with_timezone", new Properties).collect()
        },
        errorClass = "UNRECOGNIZED_SQL_TYPE",
        parameters = Map("typeName" -> "TIMESTAMP WITH TIME ZONE", "jdbcType" -> "-101"))
    }
  }

  test("Column TIMESTAMP with TIME ZONE(JVM timezone)") {
    def checkRow(row: Row, ts: String): Unit = {
      assert(row.getTimestamp(1).equals(Timestamp.valueOf(ts)))
    }

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZone.getDefault.getID) {
      val dfRead = sqlContext.read.jdbc(jdbcUrl, "ts_with_timezone", new Properties)
      withDefaultTimeZone(PST) {
        assert(dfRead.collect().toSet ===
          Set(
            Row(BigDecimal.valueOf(1), java.sql.Timestamp.valueOf("1999-12-01 03:00:00")),
            Row(BigDecimal.valueOf(2), java.sql.Timestamp.valueOf("1999-12-01 12:00:00"))))
      }

      withDefaultTimeZone(UTC) {
        assert(dfRead.collect().toSet ===
          Set(
            Row(BigDecimal.valueOf(1), java.sql.Timestamp.valueOf("1999-12-01 11:00:00")),
            Row(BigDecimal.valueOf(2), java.sql.Timestamp.valueOf("1999-12-01 20:00:00"))))
      }
    }
  }

  test("SPARK-18004: Make sure date or timestamp related predicate is pushed down correctly") {
    val props = new Properties()
    props.put("oracle.jdbc.mapDateToTimestamp", "false")

    val schema = StructType(Seq(
      StructField("date_type", DateType, true),
      StructField("timestamp_type", TimestampType, true)
    ))

    val tableName = "test_date_timestamp_pushdown"
    val dateVal = Date.valueOf("2017-06-22")
    val timestampVal = Timestamp.valueOf("2017-06-22 21:30:07")

    val data = spark.sparkContext.parallelize(Seq(
      Row(dateVal, timestampVal)
    ))

    val dfWrite = spark.createDataFrame(data, schema)
    dfWrite.write.jdbc(jdbcUrl, tableName, props)

    val dfRead = spark.read.jdbc(jdbcUrl, tableName, props)

    val millis = System.currentTimeMillis()
    val dt = new java.sql.Date(millis)
    val ts = new java.sql.Timestamp(millis)

    // Query Oracle table with date and timestamp predicates
    // which should be pushed down to Oracle.
    val df = dfRead.filter(dfRead.col("date_type").lt(dt))
      .filter(dfRead.col("timestamp_type").lt(ts))

    val parentPlan = df.queryExecution.executedPlan
    assert(parentPlan.isInstanceOf[WholeStageCodegenExec])
    val node = parentPlan.asInstanceOf[WholeStageCodegenExec]
    val metadata = node.child.asInstanceOf[RowDataSourceScanExec].metadata
    // The "PushedFilters" part should exist in Dataframe's
    // physical plan and the existence of right literals in
    // "PushedFilters" is used to prove that the predicates
    // pushing down have been effective.
    assert(metadata.get("PushedFilters").isDefined)
    assert(metadata("PushedFilters").contains(dt.toString))
    assert(metadata("PushedFilters").contains(ts.toString))

    val row = df.collect()(0)
    assert(row.getDate(0).equals(dateVal))
    assert(row.getTimestamp(1).equals(timestampVal))
  }

  test("SPARK-20427/SPARK-20921: read table use custom schema by jdbc api") {
    // default will throw IllegalArgumentException
    val e = intercept[org.apache.spark.SparkException] {
      spark.read.jdbc(jdbcUrl, "tableWithCustomSchema", new Properties()).collect()
    }
    assert(e.getCause().isInstanceOf[ArithmeticException])
    assert(e.getMessage.contains("Decimal precision 39 exceeds max precision 38"))

    // custom schema can read data
    val props = new Properties()
    props.put("customSchema",
      s"ID DECIMAL(${DecimalType.MAX_PRECISION}, 0), N1 INT, N2 BOOLEAN")
    val dfRead = spark.read.jdbc(jdbcUrl, "tableWithCustomSchema", props)

    val rows = dfRead.collect()
    // verify the data type
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(0).equals("class java.math.BigDecimal"))
    assert(types(1).equals("class java.lang.Integer"))
    assert(types(2).equals("class java.lang.Boolean"))

    // verify the value
    val values = rows(0)
    assert(values.getDecimal(0).equals(new java.math.BigDecimal("12312321321321312312312312123")))
    assert(values.getInt(1).equals(1))
    assert(values.getBoolean(2) == false)
  }

  test("SPARK-22303: handle BINARY_DOUBLE and BINARY_FLOAT as DoubleType and FloatType") {
    val tableName = "oracle_types"
    val schema = StructType(Seq(
      StructField("d", DoubleType, true),
      StructField("f", FloatType, true)))
    val props = new Properties()

    // write it back to the table (append mode)
    val data = spark.sparkContext.parallelize(Seq(Row(1.1, 2.2f)))
    val dfWrite = spark.createDataFrame(data, schema)
    dfWrite.write.mode(SaveMode.Append).jdbc(jdbcUrl, tableName, props)

    // read records from oracle_types
    val dfRead = sqlContext.read.jdbc(jdbcUrl, tableName, new Properties)
    val rows = dfRead.collect()
    assert(rows.size == 1)

    // check data types
    val types = dfRead.schema.map(field => field.dataType)
    assert(types(0).equals(DoubleType))
    assert(types(1).equals(FloatType))

    // check values
    val values = rows(0)
    assert(values.getDouble(0) === 1.1)
    assert(values.getFloat(1) === 2.2f)
  }

  test("SPARK-22814 support date/timestamp types in partitionColumn") {
    val expectedResult = Set(
      (1, "2018-07-06", "2018-07-06 05:50:00"),
      (2, "2018-07-06", "2018-07-06 08:10:08"),
      (3, "2018-07-08", "2018-07-08 13:32:01"),
      (4, "2018-07-12", "2018-07-12 09:51:15")
    ).map { case (id, date, timestamp) =>
      Row(BigDecimal.valueOf(id), Date.valueOf(date), Timestamp.valueOf(timestamp))
    }

    // DateType partition column
    val df1 = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "datetimePartitionTest")
      .option("partitionColumn", "d")
      .option("lowerBound", "2018-07-06")
      .option("upperBound", "2018-07-20")
      .option("numPartitions", 3)
      // oracle.jdbc.mapDateToTimestamp defaults to true. If this flag is not disabled, column d
      // (Oracle DATE) will be resolved as Catalyst Timestamp, which will fail bound evaluation of
      // the partition column. E.g. 2018-07-06 cannot be evaluated as Timestamp, and the error
      // message says: Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff].
      .option("oracle.jdbc.mapDateToTimestamp", "false")
      .option("sessionInitStatement", "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
      .load()

    df1.logicalPlan match {
      case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
        val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
        assert(whereClauses === Set(
          """"D" < '2018-07-11' or "D" is null""",
          """"D" >= '2018-07-11' AND "D" < '2018-07-15'""",
          """"D" >= '2018-07-15'"""))
    }
    assert(df1.collect.toSet === expectedResult)

    // TimestampType partition column
    val df2 = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "datetimePartitionTest")
      .option("partitionColumn", "t")
      .option("lowerBound", "2018-07-04 03:30:00.0")
      .option("upperBound", "2018-07-27 14:11:05.0")
      .option("numPartitions", 2)
      .option("oracle.jdbc.mapDateToTimestamp", "false")
      .option("sessionInitStatement",
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")
      .load()

    df2.logicalPlan match {
      case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
        val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
        assert(whereClauses === Set(
          """"T" < '2018-07-15 20:50:32.5' or "T" is null""",
          """"T" >= '2018-07-15 20:50:32.5'"""))
    }
    assert(df2.collect.toSet === expectedResult)
  }

  test("query JDBC option") {
    val expectedResult = Set(
      (1, "1991-11-09", "1996-01-01 01:23:45")
    ).map { case (id, date, timestamp) =>
      Row(BigDecimal.valueOf(id), Date.valueOf(date), Timestamp.valueOf(timestamp))
    }

    val query = "SELECT id, d, t FROM datetime WHERE id = 1"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", query)
      .option("oracle.jdbc.mapDateToTimestamp", "false")
      .load()
    assert(df.collect.toSet === expectedResult)

    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$jdbcUrl',
         |   query '$query',
         |   oracle.jdbc.mapDateToTimestamp false)
       """.stripMargin.replaceAll("\n", " "))
    assert(sql("select id, d, t from queryOption").collect.toSet == expectedResult)
  }

  test("SPARK-32992: map Oracle's ROWID type to StringType") {
    val rows = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", "SELECT ROWID from datetime")
      .load()
      .collect()
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(0).equals("class java.lang.String"))
    assert(!rows(0).getString(0).isEmpty)
  }

  test("SPARK-44885: query row with ROWID type containing NULL value") {
    val rows = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      // Rename column to `row_id` to prevent the following SQL error:
      //   ORA-01446: cannot select ROWID from view with DISTINCT, GROUP BY, etc.
      // See also https://stackoverflow.com/a/42632686/13300239
      .option("query", "SELECT rowid as row_id from datetime where d = {d '1991-11-09'}\n" +
        "union all\n" +
        "select null from dual")
      .load()
      .collect()
    assert(rows(0).getString(0).nonEmpty)
    assert(rows(1).getString(0) == null)
  }
}
