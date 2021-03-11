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
      assert(msg.contains("ORC data source does not support interval data type."))

      // read path
      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", CalendarIntervalType, true) :: Nil)
        spark.range(1).write.mode("overwrite").orc(orcDir)
        spark.read.schema(schema).orc(orcDir).collect()
      }.getMessage
      assert(msg.contains("ORC data source does not support interval data type."))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", new IntervalUDT(), true) :: Nil)
        spark.range(1).write.mode("overwrite").orc(orcDir)
        spark.read.schema(schema).orc(orcDir).collect()
      }.getMessage
      assert(msg.contains("ORC data source does not support interval data type."))
    }
  }

  test("Check BloomFilter creation") {
    Seq(true, false).foreach { convertMetastore =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> s"$convertMetastore") {
        testBloomFilterCreation(org.apache.orc.OrcProto.Stream.Kind.BLOOM_FILTER_UTF8)
      }
    }
  }

  test("Enforce direct encoding column-wise selectively") {
    Seq(true, false).foreach { convertMetastore =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> s"$convertMetastore") {
        testSelectiveDictionaryEncoding(isSelective = false, isHiveOrc = true)
      }
    }
  }

  test("SPARK-11412 read and merge orc schemas in parallel") {
    testMergeSchemasInParallel(OrcFileOperator.readOrcSchemasInParallel)
  }

  test("SPARK-25993 CREATE EXTERNAL TABLE with subdirectories") {
    Seq(true, false).foreach { convertMetastore =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> s"$convertMetastore") {
        withTempDir { dir =>
          withTable("orc_tbl1", "orc_tbl2", "orc_tbl3") {
            val orcTblStatement1 =
              s"""
                 |CREATE EXTERNAL TABLE orc_tbl1(
                 |  c1 int,
                 |  c2 int,
                 |  c3 string)
                 |STORED AS orc
                 |LOCATION '${s"${dir.getCanonicalPath}/l1/"}'""".stripMargin
            sql(orcTblStatement1)

            val orcTblInsertL1 =
              s"INSERT INTO TABLE orc_tbl1 VALUES (1, 1, 'orc1'), (2, 2, 'orc2')".stripMargin
            sql(orcTblInsertL1)

            val orcTblStatement2 =
            s"""
               |CREATE EXTERNAL TABLE orc_tbl2(
               |  c1 int,
               |  c2 int,
               |  c3 string)
               |STORED AS orc
               |LOCATION '${s"${dir.getCanonicalPath}/l1/l2/"}'""".stripMargin
            sql(orcTblStatement2)

            val orcTblInsertL2 =
              s"INSERT INTO TABLE orc_tbl2 VALUES (3, 3, 'orc3'), (4, 4, 'orc4')".stripMargin
            sql(orcTblInsertL2)

            val orcTblStatement3 =
            s"""
               |CREATE EXTERNAL TABLE orc_tbl3(
               |  c1 int,
               |  c2 int,
               |  c3 string)
               |STORED AS orc
               |LOCATION '${s"${dir.getCanonicalPath}/l1/l2/l3/"}'""".stripMargin
            sql(orcTblStatement3)

            val orcTblInsertL3 =
              s"INSERT INTO TABLE orc_tbl3 VALUES (5, 5, 'orc5'), (6, 6, 'orc6')".stripMargin
            sql(orcTblInsertL3)

            withTable("tbl1", "tbl2", "tbl3", "tbl4", "tbl5", "tbl6") {
              val topDirStatement =
                s"""
                   |CREATE EXTERNAL TABLE tbl1(
                   |  c1 int,
                   |  c2 int,
                   |  c3 string)
                   |STORED AS orc
                   |LOCATION '${s"${dir.getCanonicalPath}"}'""".stripMargin
              sql(topDirStatement)
              val topDirSqlStatement = s"SELECT * FROM tbl1"
              if (convertMetastore) {
                checkAnswer(sql(topDirSqlStatement), Nil)
              } else {
                checkAnswer(sql(topDirSqlStatement), (1 to 6).map(i => Row(i, i, s"orc$i")))
              }

              val l1DirStatement =
                s"""
                   |CREATE EXTERNAL TABLE tbl2(
                   |  c1 int,
                   |  c2 int,
                   |  c3 string)
                   |STORED AS orc
                   |LOCATION '${s"${dir.getCanonicalPath}/l1/"}'""".stripMargin
              sql(l1DirStatement)
              val l1DirSqlStatement = s"SELECT * FROM tbl2"
              if (convertMetastore) {
                checkAnswer(sql(l1DirSqlStatement), (1 to 2).map(i => Row(i, i, s"orc$i")))
              } else {
                checkAnswer(sql(l1DirSqlStatement), (1 to 6).map(i => Row(i, i, s"orc$i")))
              }

              val l2DirStatement =
                s"""
                   |CREATE EXTERNAL TABLE tbl3(
                   |  c1 int,
                   |  c2 int,
                   |  c3 string)
                   |STORED AS orc
                   |LOCATION '${s"${dir.getCanonicalPath}/l1/l2/"}'""".stripMargin
              sql(l2DirStatement)
              val l2DirSqlStatement = s"SELECT * FROM tbl3"
              if (convertMetastore) {
                checkAnswer(sql(l2DirSqlStatement), (3 to 4).map(i => Row(i, i, s"orc$i")))
              } else {
                checkAnswer(sql(l2DirSqlStatement), (3 to 6).map(i => Row(i, i, s"orc$i")))
              }

              val wildcardTopDirStatement =
                s"""
                   |CREATE EXTERNAL TABLE tbl4(
                   |  c1 int,
                   |  c2 int,
                   |  c3 string)
                   |STORED AS orc
                   |LOCATION '${new File(s"${dir}/*").toURI}'""".stripMargin
              sql(wildcardTopDirStatement)
              val wildcardTopDirSqlStatement = s"SELECT * FROM tbl4"
              if (convertMetastore) {
                checkAnswer(sql(wildcardTopDirSqlStatement), (1 to 2).map(i => Row(i, i, s"orc$i")))
              } else {
                checkAnswer(sql(wildcardTopDirSqlStatement), Nil)
              }

              val wildcardL1DirStatement =
                s"""
                   |CREATE EXTERNAL TABLE tbl5(
                   |  c1 int,
                   |  c2 int,
                   |  c3 string)
                   |STORED AS orc
                   |LOCATION '${new File(s"${dir}/l1/*").toURI}'""".stripMargin
              sql(wildcardL1DirStatement)
              val wildcardL1DirSqlStatement = s"SELECT * FROM tbl5"
              if (convertMetastore) {
                checkAnswer(sql(wildcardL1DirSqlStatement), (1 to 4).map(i => Row(i, i, s"orc$i")))
              } else {
                checkAnswer(sql(wildcardL1DirSqlStatement), Nil)
              }

              val wildcardL2Statement =
                s"""
                   |CREATE EXTERNAL TABLE tbl6(
                   |  c1 int,
                   |  c2 int,
                   |  c3 string)
                   |STORED AS orc
                   |LOCATION '${new File(s"${dir}/l1/l2/*").toURI}'""".stripMargin
              sql(wildcardL2Statement)
              val wildcardL2SqlStatement = s"SELECT * FROM tbl6"
              if (convertMetastore) {
                checkAnswer(sql(wildcardL2SqlStatement), (3 to 6).map(i => Row(i, i, s"orc$i")))
              } else {
                checkAnswer(sql(wildcardL2SqlStatement), Nil)
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-31580: Read a file written before ORC-569") {
    // Test ORC file came from ORC-621
    val df = readResourceOrcFile("test-data/TestStringDictionary.testRowIndex.orc")
    assert(df.where("str < 'row 001000'").count() === 1000)
  }
}
