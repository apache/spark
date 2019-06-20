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

package org.apache.spark.sql.hive.orc

import java.io.File

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.TestingUDT.{IntervalData, IntervalUDT}
import org.apache.spark.sql.execution.datasources.orc.OrcSuite
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class HiveOrcSourceSuite extends OrcSuite with TestHiveSingleton {

  override val orcImp: String = "hive"

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql(
      s"""CREATE EXTERNAL TABLE normal_orc(
         |  intField INT,
         |  stringField STRING
         |)
         |STORED AS ORC
         |LOCATION '${orcTableAsDir.toURI}'
       """.stripMargin)

    sql(
      s"""INSERT INTO TABLE normal_orc
         |SELECT intField, stringField FROM orc_temp_table
       """.stripMargin)

    spark.sql(
      s"""CREATE TEMPORARY VIEW normal_orc_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).toURI}'
         |)
       """.stripMargin)

    spark.sql(
      s"""CREATE TEMPORARY VIEW normal_orc_as_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).toURI}'
         |)
       """.stripMargin)
  }

  test("SPARK-19459/SPARK-18220: read char/varchar column written by Hive") {
    val location = Utils.createTempDir()
    val uri = location.toURI
    try {
      hiveClient.runSqlHive("USE default")
      hiveClient.runSqlHive(
        """
          |CREATE EXTERNAL TABLE hive_orc(
          |  a STRING,
          |  b CHAR(10),
          |  c VARCHAR(10),
          |  d ARRAY<CHAR(3)>)
          |STORED AS orc""".stripMargin)
      // Hive throws an exception if I assign the location in the create table statement.
      hiveClient.runSqlHive(
        s"ALTER TABLE hive_orc SET LOCATION '$uri'")
      hiveClient.runSqlHive(
        """
          |INSERT INTO TABLE hive_orc
          |SELECT 'a', 'b', 'c', ARRAY(CAST('d' AS CHAR(3)))
          |FROM (SELECT 1) t""".stripMargin)

      // We create a different table in Spark using the same schema which points to
      // the same location.
      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE spark_orc(
           |  a STRING,
           |  b CHAR(10),
           |  c VARCHAR(10),
           |  d ARRAY<CHAR(3)>)
           |STORED AS orc
           |LOCATION '$uri'""".stripMargin)
      val result = Row("a", "b         ", "c", Seq("d  "))
      checkAnswer(spark.table("hive_orc"), result)
      checkAnswer(spark.table("spark_orc"), result)
    } finally {
      hiveClient.runSqlHive("DROP TABLE IF EXISTS hive_orc")
      hiveClient.runSqlHive("DROP TABLE IF EXISTS spark_orc")
      Utils.deleteRecursively(location)
    }
  }

  test("SPARK-24204 error handling for unsupported data types") {
    withTempDir { dir =>
      val orcDir = new File(dir, "orc").getCanonicalPath

      // write path
      var msg = intercept[AnalysisException] {
        sql("select interval 1 days").write.mode("overwrite").orc(orcDir)
      }.getMessage
      assert(msg.contains("Cannot save interval data type into external storage."))

      msg = intercept[AnalysisException] {
        sql("select null").write.mode("overwrite").orc(orcDir)
      }.getMessage
      assert(msg.contains("ORC data source does not support null data type."))

      msg = intercept[AnalysisException] {
        spark.udf.register("testType", () => new IntervalData())
        sql("select testType()").write.mode("overwrite").orc(orcDir)
      }.getMessage
      assert(msg.contains("ORC data source does not support calendarinterval data type."))

      // read path
      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", CalendarIntervalType, true) :: Nil)
        spark.range(1).write.mode("overwrite").orc(orcDir)
        spark.read.schema(schema).orc(orcDir).collect()
      }.getMessage
      assert(msg.contains("ORC data source does not support calendarinterval data type."))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", new IntervalUDT(), true) :: Nil)
        spark.range(1).write.mode("overwrite").orc(orcDir)
        spark.read.schema(schema).orc(orcDir).collect()
      }.getMessage
      assert(msg.contains("ORC data source does not support calendarinterval data type."))
    }
  }

  test("Check BloomFilter creation") {
    Seq(true, false).foreach { convertMetastore =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> s"$convertMetastore") {
        if (HiveUtils.isHive23) {
          testBloomFilterCreation(org.apache.orc.OrcProto.Stream.Kind.BLOOM_FILTER_UTF8)
        } else {
          // Before ORC-101
          testBloomFilterCreation(org.apache.orc.OrcProto.Stream.Kind.BLOOM_FILTER)
        }
      }
    }
  }

  test("Enforce direct encoding column-wise selectively") {
    Seq(true, false).foreach { convertMetastore =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> s"$convertMetastore") {
        testSelectiveDictionaryEncoding(isSelective = false, isHive23 = HiveUtils.isHive23)
      }
    }
  }
}
