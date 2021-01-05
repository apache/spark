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

package org.apache.spark.sql.hive.execution

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.HiveUtils.{CONVERT_METASTORE_ORC, CONVERT_METASTORE_PARQUET}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf.ORC_IMPLEMENTATION
import org.apache.spark.sql.test.SQLTestUtils

class HiveSerDeReadWriteSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  private var originalConvertMetastoreParquet = CONVERT_METASTORE_PARQUET.defaultValueString
  private var originalConvertMetastoreORC = CONVERT_METASTORE_ORC.defaultValueString
  private var originalORCImplementation = ORC_IMPLEMENTATION.defaultValueString

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalConvertMetastoreParquet = spark.conf.get(CONVERT_METASTORE_PARQUET.key)
    originalConvertMetastoreORC = spark.conf.get(CONVERT_METASTORE_ORC.key)
    originalORCImplementation = spark.conf.get(ORC_IMPLEMENTATION)

    spark.conf.set(CONVERT_METASTORE_PARQUET.key, "false")
    spark.conf.set(CONVERT_METASTORE_ORC.key, "false")
    spark.conf.set(ORC_IMPLEMENTATION.key, "hive")
  }

  protected override def afterAll(): Unit = {
    spark.conf.set(CONVERT_METASTORE_PARQUET.key, originalConvertMetastoreParquet)
    spark.conf.set(CONVERT_METASTORE_ORC.key, originalConvertMetastoreORC)
    spark.conf.set(ORC_IMPLEMENTATION.key, originalORCImplementation)
    super.afterAll()
  }

  private def checkNumericTypes(fileFormat: String, dataType: String, value: Any): Unit = {
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 $dataType) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values(1)")
      checkAnswer(spark.table("hive_serde"), Row(1))
      spark.sql(s"INSERT INTO TABLE hive_serde values($value)")
      checkAnswer(spark.table("hive_serde"), Seq(Row(1), Row(value)))
    }
  }

  private def checkDateTimeTypes(fileFormat: String): Unit = {
    // TIMESTAMP
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 TIMESTAMP) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values('2019-04-11 15:50:00')")
      checkAnswer(spark.table("hive_serde"), Row(Timestamp.valueOf("2019-04-11 15:50:00")))
      spark.sql("INSERT INTO TABLE hive_serde values(TIMESTAMP('2019-04-12 15:50:00'))")
      checkAnswer(
        spark.table("hive_serde"),
        Seq(Row(Timestamp.valueOf("2019-04-11 15:50:00")),
          Row(Timestamp.valueOf("2019-04-12 15:50:00"))))
    }

    // DATE
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 DATE) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values('2019-04-11')")
      checkAnswer(spark.table("hive_serde"), Row(Date.valueOf("2019-04-11")))
      spark.sql("INSERT INTO TABLE hive_serde values(TIMESTAMP('2019-04-12'))")
      checkAnswer(
        spark.table("hive_serde"),
        Seq(Row(Date.valueOf("2019-04-11")), Row(Date.valueOf("2019-04-12"))))
    }
  }

  private def checkStringTypes(fileFormat: String, dataType: String, value: String): Unit = {
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 $dataType) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values('s')")
      checkAnswer(spark.table("hive_serde"), Row("s"))
      spark.sql(s"INSERT INTO TABLE hive_serde values('$value')")
      checkAnswer(spark.table("hive_serde"), Seq(Row("s"), Row(value)))
    }
  }

  private def checkCharTypes(fileFormat: String): Unit = {
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 CHAR(10)) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values('s')")
      checkAnswer(spark.table("hive_serde"), Row("s" + " " * 9))
      spark.sql(s"INSERT INTO TABLE hive_serde values('s3')")
      checkAnswer(spark.table("hive_serde"), Seq(Row("s" + " " * 9), Row("s3" + " " * 8)))
    }
  }

  private def checkMiscTypes(fileFormat: String): Unit = {
    // BOOLEAN
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 BOOLEAN) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values(false)")
      checkAnswer(spark.table("hive_serde"), Row(false))
      spark.sql("INSERT INTO TABLE hive_serde values(true)")
      checkAnswer(spark.table("hive_serde"), Seq(Row(false), Row(true)))
    }

    // BINARY
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 BINARY) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde values('1')")
      checkAnswer(spark.table("hive_serde"), Row("1".getBytes))
      spark.sql("INSERT INTO TABLE hive_serde values(BINARY('2'))")
      checkAnswer(spark.table("hive_serde"), Seq(Row("1".getBytes), Row("2".getBytes)))
    }
  }

  private def checkComplexTypes(fileFormat: String): Unit = {
    // ARRAY<data_type>
    withTable("hive_serde") {
      hiveClient.runSqlHive(s"CREATE TABLE hive_serde (c1 ARRAY <STRING>) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde SELECT ARRAY('a','b') FROM (SELECT 1) t")
      checkAnswer(spark.table("hive_serde"), Row(Array("a", "b")))
      spark.sql("INSERT INTO TABLE hive_serde SELECT ARRAY('c', 'd')")
      checkAnswer(spark.table("hive_serde"), Seq(Row(Array("a", "b")), Row(Array("c", "d"))))
    }
    // MAP<primitive_type, data_type>
    withTable("hive_serde") {
      hiveClient.runSqlHive(
        s"CREATE TABLE hive_serde (c1 MAP <STRING, STRING>) STORED AS $fileFormat")
      hiveClient.runSqlHive("INSERT INTO TABLE hive_serde SELECT MAP('1', 'a') FROM (SELECT 1) t")
      checkAnswer(spark.table("hive_serde"), Row(Map("1" -> "a")))
      spark.sql("INSERT INTO TABLE hive_serde SELECT MAP('2', 'b')")
      checkAnswer(spark.table("hive_serde"), Seq(Row(Map("1" -> "a")), Row(Map("2" -> "b"))))
    }

    // STRUCT<col_name : data_type [COMMENT col_comment], ...>
    withTable("hive_serde") {
      hiveClient.runSqlHive(
        s"CREATE TABLE hive_serde (c1 STRUCT <k: INT>) STORED AS $fileFormat")
      hiveClient.runSqlHive(
        "INSERT INTO TABLE hive_serde SELECT NAMED_STRUCT('k', 1) FROM (SELECT 1) t")
      checkAnswer(spark.table("hive_serde"), Row(Row(1)))
      spark.sql("INSERT INTO TABLE hive_serde SELECT NAMED_STRUCT('k', 2)")
      checkAnswer(spark.table("hive_serde"), Seq(Row(Row(1)), Row(Row(2))))
    }
  }

  Seq("SEQUENCEFILE", "TEXTFILE", "RCFILE", "ORC", "PARQUET", "AVRO").foreach { fileFormat =>
    test(s"Read/Write Hive $fileFormat serde table") {
      // Numeric Types
      checkNumericTypes(fileFormat, "TINYINT", 2)
      checkNumericTypes(fileFormat, "SMALLINT", 2)
      checkNumericTypes(fileFormat, "INT", 2)
      checkNumericTypes(fileFormat, "BIGINT", 2)
      checkNumericTypes(fileFormat, "FLOAT", 2.1F)
      checkNumericTypes(fileFormat, "DOUBLE", 2.1D)
      checkNumericTypes(fileFormat, "DECIMAL(9, 2)", 2.1D)
      checkNumericTypes(fileFormat, "DECIMAL(18, 2)", 2.1D)
      checkNumericTypes(fileFormat, "DECIMAL(38, 2)", 2.1D)

      // Date/Time Types
      // SPARK-28885 String value is not allowed to be stored as date/timestamp type with
      // ANSI store assignment policy.
      checkDateTimeTypes(fileFormat)

      // String Types
      checkStringTypes(fileFormat, "STRING", "s1")
      checkStringTypes(fileFormat, "VARCHAR(10)", "s2")
      checkCharTypes(fileFormat)

      // Misc Types
      checkMiscTypes(fileFormat)

      // Complex Types
      checkComplexTypes(fileFormat)
    }
  }
}
