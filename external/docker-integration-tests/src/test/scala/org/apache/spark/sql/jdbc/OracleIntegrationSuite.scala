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

import java.sql.{Connection, Date, Timestamp}
import java.util.Properties
import java.math.BigDecimal

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * This patch was tested using the Oracle docker. Created this integration suite for the same.
 * The ojdbc6-11.2.0.2.0.jar was to be downloaded from the maven repository. Since there was
 * no jdbc jar available in the maven repository, the jar was downloaded from oracle site
 * manually and installed in the local; thus tested. So, for SparkQA test case run, the
 * ojdbc jar might be manually placed in the local maven repository(com/oracle/ojdbc6/11.2.0.2.0)
 * while Spark QA test run.
 *
 * The following would be the steps to test this
 * 1. Pull oracle 11g image - docker pull wnameless/oracle-xe-11g
 * 2. Start docker - sudo service docker start
 * 3. Download oracle 11g driver jar and put it in maven local repo:
 *    (com/oracle/ojdbc6/11.2.0.2.0/ojdbc6-11.2.0.2.0.jar)
 * 4. The timeout and interval parameter to be increased from 60,1 to a high value for oracle test
 *    in DockerJDBCIntegrationSuite.scala (Locally tested with 200,200 and executed successfully).
 * 5. Run spark test - ./build/sbt "test-only org.apache.spark.sql.jdbc.OracleIntegrationSuite"
 *
 * All tests in this suite are ignored because of the dependency with the oracle jar from maven
 * repository.
 */
@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSQLContext {
  import testImplicits._

  override val db = new DatabaseOnDocker {
    override val imageName = "wnameless/oracle-xe-11g:14.04.4"
    override val env = Map(
      "ORACLE_ROOT_PASSWORD" -> "oracle"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/oracle@//$ip:$port/xe"
    override def getStartupProcessName: Option[String] = None
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE TABLE datetime (id NUMBER(10), d DATE, t TIMESTAMP)")
      .executeUpdate()
    conn.prepareStatement(
      """INSERT INTO datetime VALUES
        |(1, {d '1991-11-09'}, {ts '1996-01-01 01:23:45'})
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
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


    conn.prepareStatement("CREATE TABLE numerics (b DECIMAL(1), f DECIMAL(3, 2), i DECIMAL(10))").executeUpdate();
    conn.prepareStatement(
      "INSERT INTO numerics VALUES (4, 1.23, 9999999999)").executeUpdate();
    conn.commit();
  }


  test("SPARK-16625 : Importing Oracle numeric types") { 
    val df = sqlContext.read.jdbc(jdbcUrl, "numerics", new Properties);
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
    assert(row.getDecimal(2).compareTo(BigDecimal.valueOf(9999999999l)) == 0)
  }


  test("SPARK-12941: String datatypes to be mapped to Varchar in Oracle") {
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
}
