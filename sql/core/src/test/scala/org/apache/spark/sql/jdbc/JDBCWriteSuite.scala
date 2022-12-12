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

import java.sql.{Date, DriverManager, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Properties

import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCWriteSuite extends SharedSparkSession with BeforeAndAfter {

  val url = "jdbc:h2:mem:testdb2"
  var conn: java.sql.Connection = null
  val url1 = "jdbc:h2:mem:testdb3"
  var conn1: java.sql.Connection = null
  val properties = new Properties()
  properties.setProperty("user", "testUser")
  properties.setProperty("password", "testPass")
  properties.setProperty("rowId", "false")

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2")
    override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
  }

  before {
    Utils.classForName("org.h2.Driver")
    conn = DriverManager.getConnection(url)
    conn.prepareStatement("create schema test").executeUpdate()

    conn1 = DriverManager.getConnection(url1, properties)
    conn1.prepareStatement("create schema test").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people").executeUpdate()
    conn1.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people1").executeUpdate()
    conn1.prepareStatement(
      "create table test.people1 (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.commit()

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW PEOPLE
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW PEOPLE1
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE1', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    conn1.prepareStatement("create table test.timetypes (d DATE, t TIMESTAMP)").executeUpdate()
    conn.commit()
  }

  after {
    conn.close()
    conn1.close()
  }

  private lazy val arr2x2 = Array[Row](Row.apply("dave", 42), Row.apply("mary", 222))
  private lazy val arr1x2 = Array[Row](Row.apply("fred", 3))
  private lazy val schema2 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) :: Nil)

  private lazy val arr2x3 = Array[Row](Row.apply("dave", 42, 1), Row.apply("mary", 222, 2))
  private lazy val schema3 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) ::
      StructField("seq", IntegerType) :: Nil)

  private lazy val schema4 = StructType(
      StructField("NAME", StringType) ::
      StructField("ID", IntegerType) :: Nil)

  test("Basic CREATE") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    df.write.jdbc(url, "TEST.BASICCREATETEST", new Properties())
    assert(2 === spark.read.jdbc(url, "TEST.BASICCREATETEST", new Properties()).count())
    assert(
      2 === spark.read.jdbc(url, "TEST.BASICCREATETEST", new Properties()).collect()(0).length)
  }

  test("Basic CREATE with illegal batchsize") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    (-1 to 0).foreach { size =>
      val properties = new Properties()
      properties.setProperty(JDBCOptions.JDBC_BATCH_INSERT_SIZE, size.toString)
      val e = intercept[IllegalArgumentException] {
        df.write.mode(SaveMode.Overwrite).jdbc(url, "TEST.BASICCREATETEST", properties)
      }.getMessage
      assert(e.contains(s"Invalid value `$size` for parameter `batchsize`"))
    }
  }

  test("Basic CREATE with batchsize") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    (1 to 3).foreach { size =>
      val properties = new Properties()
      properties.setProperty(JDBCOptions.JDBC_BATCH_INSERT_SIZE, size.toString)
      df.write.mode(SaveMode.Overwrite).jdbc(url, "TEST.BASICCREATETEST", properties)
      assert(2 === spark.read.jdbc(url, "TEST.BASICCREATETEST", new Properties()).count())
    }
  }

  test("CREATE with ignore") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x3), schema3)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.mode(SaveMode.Ignore).jdbc(url1, "TEST.DROPTEST", properties)
    assert(2 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).count())
    assert(3 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)

    df2.write.mode(SaveMode.Ignore).jdbc(url1, "TEST.DROPTEST", properties)
    assert(2 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).count())
    assert(3 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)
  }

  test("CREATE with overwrite") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x3), schema3)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.jdbc(url1, "TEST.DROPTEST", properties)
    assert(2 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).count())
    assert(3 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)

    df2.write.mode(SaveMode.Overwrite).jdbc(url1, "TEST.DROPTEST", properties)
    assert(1 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).count())
    assert(2 === spark.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)
  }

  test("CREATE then INSERT to append") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.jdbc(url, "TEST.APPENDTEST", new Properties())
    df2.write.mode(SaveMode.Append).jdbc(url, "TEST.APPENDTEST", new Properties())
    assert(3 === spark.read.jdbc(url, "TEST.APPENDTEST", new Properties()).count())
    assert(2 === spark.read.jdbc(url, "TEST.APPENDTEST", new Properties()).collect()(0).length)
  }

  test("SPARK-18123 Append with column names with different cases") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema4)

    df.write.jdbc(url, "TEST.APPENDTEST", new Properties())

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          df2.write.mode(SaveMode.Append).jdbc(url, "TEST.APPENDTEST", new Properties())
        },
        errorClass = "_LEGACY_ERROR_TEMP_1156",
        parameters = Map(
          "colName" -> "NAME",
          "tableSchema" ->
            "Some(StructType(StructField(name,StringType,true),StructField(id,IntegerType,true)))"))
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      df2.write.mode(SaveMode.Append).jdbc(url, "TEST.APPENDTEST", new Properties())
      assert(3 === spark.read.jdbc(url, "TEST.APPENDTEST", new Properties()).count())
      assert(2 === spark.read.jdbc(url, "TEST.APPENDTEST", new Properties()).collect()(0).length)
    }
  }

  test("Truncate") {
    JdbcDialects.unregisterDialect(H2Dialect)
    try {
      JdbcDialects.registerDialect(testH2Dialect)
      val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
      val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)
      val df3 = spark.createDataFrame(sparkContext.parallelize(arr2x3), schema3)

      df.write.jdbc(url1, "TEST.TRUNCATETEST", properties)
      df2.write.mode(SaveMode.Overwrite).option("truncate", true)
        .jdbc(url1, "TEST.TRUNCATETEST", properties)
      assert(1 === spark.read.jdbc(url1, "TEST.TRUNCATETEST", properties).count())
      assert(2 === spark.read.jdbc(url1, "TEST.TRUNCATETEST", properties).collect()(0).length)

      checkError(
        exception = intercept[AnalysisException] {
          df3.write.mode(SaveMode.Overwrite).option("truncate", true)
            .jdbc(url1, "TEST.TRUNCATETEST", properties)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1156",
        parameters = Map(
          "colName" -> "seq",
          "tableSchema" ->
            "Some(StructType(StructField(name,StringType,true),StructField(id,IntegerType,true)))"))
    } finally {
      JdbcDialects.unregisterDialect(testH2Dialect)
      JdbcDialects.registerDialect(H2Dialect)
    }
  }

  test("createTableOptions") {
    JdbcDialects.registerDialect(testH2Dialect)
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    val m = intercept[org.h2.jdbc.JdbcSQLSyntaxErrorException] {
      df.write.option("createTableOptions", "ENGINE tableEngineName")
      .jdbc(url1, "TEST.CREATETBLOPTS", properties)
    }.getMessage
    assert(m.contains("Class \"TABLEENGINENAME\" not found"))
    JdbcDialects.unregisterDialect(testH2Dialect)
  }

  test("Incompatible INSERT to append") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr2x3), schema3)

    df.write.jdbc(url, "TEST.INCOMPATIBLETEST", new Properties())
    checkError(
      exception = intercept[AnalysisException] {
        df2.write.mode(SaveMode.Append).jdbc(url, "TEST.INCOMPATIBLETEST", new Properties())
      },
      errorClass = "_LEGACY_ERROR_TEMP_1156",
      parameters = Map(
        "colName" -> "seq",
        "tableSchema" ->
          "Some(StructType(StructField(name,StringType,true),StructField(id,IntegerType,true)))"))
  }

  test("INSERT to JDBC Datasource") {
    sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 === spark.read.jdbc(url1, "TEST.PEOPLE1", properties).count())
    assert(2 === spark.read.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }

  test("INSERT to JDBC Datasource with overwrite") {
    sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    sql("INSERT OVERWRITE TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 === spark.read.jdbc(url1, "TEST.PEOPLE1", properties).count())
    assert(2 === spark.read.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }

  test("save works for format(\"jdbc\") if url and dbtable are set") {
    val df = sqlContext.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    df.write.format("jdbc")
    .options(Map("url" -> url, "dbtable" -> "TEST.SAVETEST"))
    .save()

    assert(2 === sqlContext.read.jdbc(url, "TEST.SAVETEST", new Properties).count)
    assert(
      2 === sqlContext.read.jdbc(url, "TEST.SAVETEST", new Properties).collect()(0).length)
  }

  test("save API with SaveMode.Overwrite") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.format("jdbc")
      .option("url", url1)
      .option("dbtable", "TEST.SAVETEST")
      .options(properties.asScala)
      .save()
    df2.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url", url1)
      .option("dbtable", "TEST.SAVETEST")
      .options(properties.asScala)
      .save()
    assert(1 === spark.read.jdbc(url1, "TEST.SAVETEST", properties).count())
    assert(2 === spark.read.jdbc(url1, "TEST.SAVETEST", properties).collect()(0).length)
  }

  test("save errors if url is not specified") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    val e = intercept[RuntimeException] {
      df.write.format("jdbc")
        .option("dbtable", "TEST.SAVETEST")
        .options(properties.asScala)
        .save()
    }.getMessage
    assert(e.contains("Option 'url' is required"))
  }

  test("save errors if dbtable is not specified") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    val e1 = intercept[RuntimeException] {
      df.write.format("jdbc")
        .option("url", url1)
        .options(properties.asScala)
        .save()
    }.getMessage
    assert(e1.contains("Option 'dbtable' or 'query' is required"))

    val e2 = intercept[RuntimeException] {
      df.write.format("jdbc")
        .option("url", url1)
        .options(properties.asScala)
        .option("query", "select * from TEST.SAVETEST")
        .save()
    }.getMessage
    val msg = "Option 'dbtable' is required. Option 'query' is not applicable while writing."
    assert(e2.contains(msg))
  }

  test("save errors if wrong user/password combination") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    val e = intercept[org.h2.jdbc.JdbcSQLInvalidAuthorizationSpecException] {
      df.write.format("jdbc")
        .option("dbtable", "TEST.SAVETEST")
        .option("url", url1)
        .save()
    }.getMessage
    assert(e.contains("Wrong user name or password"))
  }

  test("save errors if partitionColumn and numPartitions and bounds not set") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    val e = intercept[java.lang.IllegalArgumentException] {
      df.write.format("jdbc")
        .option("dbtable", "TEST.SAVETEST")
        .option("url", url1)
        .option("partitionColumn", "foo")
        .save()
    }.getMessage
    assert(e.contains("When reading JDBC data sources, users need to specify all or none " +
      "for the following options: 'partitionColumn', 'lowerBound', 'upperBound', and " +
      "'numPartitions'"))
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    df.write.format("jdbc")
      .option("Url", url1)
      .option("dbtable", "TEST.SAVETEST")
      .options(properties.asScala)
      .save()
  }

  test("SPARK-18413: Use `numPartitions` JDBCOption") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val e = intercept[IllegalArgumentException] {
      df.write.format("jdbc")
        .option("dbtable", "TEST.SAVETEST")
        .option("url", url1)
        .option("user", "testUser")
        .option("password", "testPass")
        .option(s"${JDBCOptions.JDBC_NUM_PARTITIONS}", "0")
        .save()
    }.getMessage
    assert(e.contains("Invalid value `0` for parameter `numPartitions` in table writing " +
      "via JDBC. The minimum value is 1."))
  }

  test("SPARK-19318 temporary view data source option keys should be case-insensitive") {
    withTempView("people_view") {
      sql(
        s"""
          |CREATE TEMPORARY VIEW people_view
          |USING org.apache.spark.sql.jdbc
          |OPTIONS (uRl '$url1', DbTaBlE 'TEST.PEOPLE1', User 'testUser', PassWord 'testPass')
        """.stripMargin.replaceAll("\n", " "))
      sql("INSERT OVERWRITE TABLE PEOPLE_VIEW SELECT * FROM PEOPLE")
      assert(sql("select * from people_view").count() == 2)
    }
  }

  test("SPARK-10849: test schemaString - from createTableColumnTypes option values") {
    def testCreateTableColDataTypes(types: Seq[String]): Unit = {
      val colTypes = types.zipWithIndex.map { case (t, i) => (s"col$i", t) }
      val schema = colTypes
        .foldLeft(new StructType())((schema, colType) => schema.add(colType._1, colType._2))
      val createTableColTypes =
        colTypes.map { case (col, dataType) => s"$col $dataType" }.mkString(", ")

      val expectedSchemaStr =
        colTypes.map { case (col, dataType) => s""""$col" $dataType """ }.mkString(", ")

      assert(JdbcUtils.schemaString(
        schema,
        spark.sqlContext.conf.caseSensitiveAnalysis,
        url1,
        Option(createTableColTypes)) == expectedSchemaStr)
    }

    testCreateTableColDataTypes(Seq("boolean"))
    testCreateTableColDataTypes(Seq("tinyint", "smallint", "int", "bigint"))
    testCreateTableColDataTypes(Seq("float", "double"))
    testCreateTableColDataTypes(Seq("string", "char(10)", "varchar(20)"))
    testCreateTableColDataTypes(Seq("decimal(10,0)", "decimal(10,5)"))
    testCreateTableColDataTypes(Seq("date", "timestamp"))
    testCreateTableColDataTypes(Seq("binary"))
  }

  test("SPARK-10849: create table using user specified column type and verify on target table") {
    def testUserSpecifiedColTypes(
        df: DataFrame,
        createTableColTypes: String,
        expectedTypes: Map[String, String]): Unit = {
      df.write
        .mode(SaveMode.Overwrite)
        .option("createTableColumnTypes", createTableColTypes)
        .jdbc(url1, "TEST.DBCOLTYPETEST", properties)

      // verify the data types of the created table by reading the database catalog of H2
      val query =
        """
          |(SELECT column_name, data_type, character_maximum_length
          | FROM information_schema.columns WHERE table_name = 'DBCOLTYPETEST')
        """.stripMargin
      val rows = spark.read.jdbc(url1, query, properties).collect()

      rows.foreach { row =>
        val typeName = row.getString(1)
        // For CHAR and VARCHAR, we also compare the max length
        if (typeName.contains("CHAR")) {
          val charMaxLength = row.getLong(2)
          assert(expectedTypes(row.getString(0)) == s"$typeName($charMaxLength)")
        } else {
          assert(expectedTypes(row.getString(0)) == typeName)
        }
      }
    }

    val data = Seq[Row](Row(1, "dave", "Boston"))
    val schema = StructType(
      StructField("id", IntegerType) ::
        StructField("first#name", StringType) ::
        StructField("city", StringType) :: Nil)
    val df = spark.createDataFrame(sparkContext.parallelize(data), schema)

    // out-of-order
    val expected1 =
      Map("id" -> "BIGINT", "first#name" -> "CHARACTER VARYING(123)", "city" -> "CHARACTER(20)")
    testUserSpecifiedColTypes(df, "`first#name` VARCHAR(123), id BIGINT, city CHAR(20)", expected1)
    // partial schema
    val expected2 =
      Map("id" -> "INTEGER", "first#name" -> "CHARACTER VARYING(123)", "city" -> "CHARACTER(20)")
    testUserSpecifiedColTypes(df, "`first#name` VARCHAR(123), city CHAR(20)", expected2)

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      // should still respect the original column names
      val expected = Map("id" -> "INTEGER", "first#name" -> "CHARACTER VARYING(123)",
        "city" -> "CHARACTER LARGE OBJECT(9223372036854775807)")
      testUserSpecifiedColTypes(df, "`FiRsT#NaMe` VARCHAR(123)", expected)
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val schema = StructType(
        StructField("id", IntegerType) ::
          StructField("First#Name", StringType) ::
          StructField("city", StringType) :: Nil)
      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
      val expected =
        Map("id" -> "INTEGER", "First#Name" -> "CHARACTER VARYING(123)",
          "city" -> "CHARACTER LARGE OBJECT(9223372036854775807)")
      testUserSpecifiedColTypes(df, "`First#Name` VARCHAR(123)", expected)
    }
  }

  test("SPARK-10849: jdbc CreateTableColumnTypes option with invalid data type") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    checkError(
      exception = intercept[ParseException] {
        df.write.mode(SaveMode.Overwrite)
          .option("createTableColumnTypes", "name CLOB(2000)")
          .jdbc(url1, "TEST.USERDBTYPETEST", properties)
      },
      errorClass = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"CLOB(2000)\""))
  }

  test("SPARK-10849: jdbc CreateTableColumnTypes option with invalid syntax") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    checkError(
      exception = intercept[ParseException] {
        df.write.mode(SaveMode.Overwrite)
          .option("createTableColumnTypes", "`name char(20)") // incorrectly quoted column
          .jdbc(url1, "TEST.USERDBTYPETEST", properties)
      },
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'`'", "hint" -> ": extra input '`'"))
  }

  test("SPARK-10849: jdbc CreateTableColumnTypes duplicate columns") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
      val e = intercept[AnalysisException] {
        df.write.mode(SaveMode.Overwrite)
          .option("createTableColumnTypes", "name CHAR(20), id int, NaMe VARCHAR(100)")
          .jdbc(url1, "TEST.USERDBTYPETEST", properties)
      }
      checkError(
        exception = e,
        errorClass = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`name`"))
    }
  }

  test("SPARK-10849: jdbc CreateTableColumnTypes invalid columns") {
    // schema2 has the column "id" and "name"
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val msg = intercept[AnalysisException] {
        df.write.mode(SaveMode.Overwrite)
          .option("createTableColumnTypes", "firstName CHAR(20), id int")
          .jdbc(url1, "TEST.USERDBTYPETEST", properties)
      }.getMessage()
      assert(msg.contains("createTableColumnTypes option column firstName not found in " +
        "schema struct<name:string,id:int>"))
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val msg = intercept[AnalysisException] {
        df.write.mode(SaveMode.Overwrite)
          .option("createTableColumnTypes", "id int, Name VARCHAR(100)")
          .jdbc(url1, "TEST.USERDBTYPETEST", properties)
      }.getMessage()
      assert(msg.contains("createTableColumnTypes option column Name not found in " +
        "schema struct<name:string,id:int>"))
    }
  }

  test("SPARK-19726: INSERT null to a NOT NULL column") {
    val e = intercept[SparkException] {
      sql("INSERT INTO PEOPLE1 values (null, null)")
    }.getMessage
    assert(e.contains("NULL not allowed for column \"NAME\""))
  }

  ignore("SPARK-23856 Spark jdbc setQueryTimeout option") {
    // The behaviour of the option `queryTimeout` depends on how JDBC drivers implement the API
    // `setQueryTimeout`. For example, in the h2 JDBC driver, `executeBatch` invokes multiple
    // INSERT queries in a batch and `setQueryTimeout` means that the driver checks the timeout
    // of each query. In the PostgreSQL JDBC driver, `setQueryTimeout` means that the driver
    // checks the timeout of an entire batch in a driver side. So, the test below fails because
    // this test suite depends on the h2 JDBC driver and the JDBC write path internally
    // uses `executeBatch`.
    val errMsg = intercept[SparkException] {
      spark.range(10000000L).selectExpr("id AS k", "id AS v").coalesce(1).write
        .mode(SaveMode.Overwrite)
        .option("queryTimeout", 1)
        .option("batchsize", Int.MaxValue)
        .jdbc(url1, "TEST.TIMEOUTTEST", properties)
    }.getMessage
    assert(errMsg.contains("Statement was canceled or the session timed out"))
  }

  test("metrics") {
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    runAndVerifyRecordsWritten(2) {
      df.write.mode(SaveMode.Append).jdbc(url, "TEST.BASICCREATETEST", new Properties())
    }

    runAndVerifyRecordsWritten(1) {
      df2.write.mode(SaveMode.Overwrite).jdbc(url, "TEST.BASICCREATETEST", new Properties())
    }

    runAndVerifyRecordsWritten(1) {
      df2.write.mode(SaveMode.Overwrite).option("truncate", true)
        .jdbc(url, "TEST.BASICCREATETEST", new Properties())
    }

    runAndVerifyRecordsWritten(0) {
      intercept[AnalysisException] {
        df2.write.mode(SaveMode.ErrorIfExists).jdbc(url, "TEST.BASICCREATETEST", new Properties())
      }
    }

    runAndVerifyRecordsWritten(0) {
      df.write.mode(SaveMode.Ignore).jdbc(url, "TEST.BASICCREATETEST", new Properties())
    }
  }

  private def runAndVerifyRecordsWritten(expected: Long)(job: => Unit): Unit = {
    assert(expected === runAndReturnMetrics(job, _.taskMetrics.outputMetrics.recordsWritten))
  }

  private def runAndReturnMetrics(job: => Unit, collector: (SparkListenerTaskEnd) => Long): Long = {
    val taskMetrics = new ArrayBuffer[Long]()

    // Avoid receiving earlier taskEnd events
    sparkContext.listenerBus.waitUntilEmpty()

    val listener = new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskMetrics += collector(taskEnd)
      }
    }
    sparkContext.addSparkListener(listener)

    job

    sparkContext.listenerBus.waitUntilEmpty()

    sparkContext.removeSparkListener(listener)
    taskMetrics.sum
  }

  test("SPARK-34144: write and read java.time LocalDate and Instant") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val schema = new StructType().add("d", DateType).add("t", TimestampType);
      val values = Seq(Row.apply(LocalDate.parse("2020-01-01"),
        Instant.parse("2020-02-02T12:13:14.56789Z")))
      val df = spark.createDataFrame(sparkContext.makeRDD(values), schema)

      df.write.jdbc(url, "TEST.TIMETYPES", new Properties())

      val rows = spark.read.jdbc(url, "TEST.TIMETYPES", new Properties()).collect()
      assert(1 === rows.length);
      assert(rows(0).getAs[LocalDate](0) === LocalDate.parse("2020-01-01"))
      assert(rows(0).getAs[Instant](1) === Instant.parse("2020-02-02T12:13:14.56789Z"))
    }
  }

  test("SPARK-34144: write Date and Timestampt, read LocalDate and Instant") {
    val schema = new StructType().add("d", DateType).add("t", TimestampType);
    val values = Seq(Row.apply(Date.valueOf("2020-01-01"),
      Timestamp.valueOf("2020-02-02 12:13:14.56789")))
    val df = spark.createDataFrame(sparkContext.makeRDD(values), schema)

    df.write.jdbc(url, "TEST.TIMETYPES", new Properties())

    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val rows = spark.read.jdbc(url, "TEST.TIMETYPES", new Properties()).collect()
      assert(1 === rows.length);
      assert(rows(0).getAs[LocalDate](0) === LocalDate.parse("2020-01-01"))
      // 8 hour difference since Timestamp was America/Los_Angeles and Instant is GMT
      assert(rows(0).getAs[Instant](1) === Instant.parse("2020-02-02T20:13:14.56789Z"))
    }
  }

  test("SPARK-34144: write LocalDate and Instant, read Date and Timestampt") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val schema = new StructType().add("d", DateType).add("t", TimestampType);
      val values = Seq(Row.apply(LocalDate.parse("2020-01-01"),
        Instant.parse("2020-02-02T12:13:14.56789Z")))
      val df = spark.createDataFrame(sparkContext.makeRDD(values), schema)

      df.write.jdbc(url, "TEST.TIMETYPES", new Properties())
    }

    val rows = spark.read.jdbc(url, "TEST.TIMETYPES", new Properties()).collect()
    assert(1 === rows.length);
    assert(rows(0).getAs[java.sql.Date](0) === java.sql.Date.valueOf("2020-01-01"))
    // -8 hour difference since Instant was GMT and Timestamp is America/Los_Angeles
    assert(rows(0).getAs[java.sql.Timestamp](1)
      === java.sql.Timestamp.valueOf("2020-02-02 04:13:14.56789"))
  }
}
