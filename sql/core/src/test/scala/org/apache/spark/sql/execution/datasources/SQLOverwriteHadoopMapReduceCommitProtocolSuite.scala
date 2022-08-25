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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class SQLOverwriteHadoopMapReduceCommitProtocolSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("SPARK-36571: Check staging dir") {
    val path = new Path(Utils.createTempDir().toString)
    val commitProtocol =
      new SQLOverwriteHadoopMapReduceCommitProtocol("000001", path.toString, true)
    assert(commitProtocol.stagingDir.getParent == path.getParent)
    assert(commitProtocol.stagingDir.getName == s".${path.getName}-spark-staging-000001")
  }

  test("SPARK-36571: Non-partitioned table insert overwrite") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SQLOverwriteHadoopMapReduceCommitProtocol].getName) {
      withTable("t") {
        withTempView("temp") {
          sql(
            s"""
               | CREATE TABLE t(c1 int, p1 int) USING PARQUET
            """.stripMargin)

          val df = Seq((1, 2), (1, 2))
            .toDF("c1", "p1").repartition(1)
          df.createOrReplaceTempView("temp")
          sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"), df)

          // test can delete data correctly
          sql("INSERT INTO TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"),
            Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil)

          // test can delete data correctly
          sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"), df)

        }
      }
    }
  }

  test("SPARK-36571: Partitioned table insert single partition") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SQLOverwriteHadoopMapReduceCommitProtocol].getName) {
      withTable("t") {
        withTempView("temp") {
          sql(
            s"""
               | CREATE TABLE t(c1 int, p1 string, p2 string)
               | USING PARQUET
               | PARTITIONED BY (p1, p2)
            """.stripMargin)

          val df = Seq(1, 2, 3).toDF("c1")
          df.createOrReplaceTempView("temp")
          sql("INSERT OVERWRITE TABLE t PARTITION (p1 = 1, p2 = 1) SELECT * FROM temp")
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 1 AND p2 = 1"), df)

          // test won't delete other partitions data
          sql("INSERT OVERWRITE TABLE t PARTITION (p1 = 2, p2 = 2) SELECT * FROM temp")
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 2 AND p2 = 2"), df)
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 1 AND p2 = 1"), df)

          // test can delete data correctly
          sql("INSERT OVERWRITE TABLE t PARTITION (p1 = 1, p2 = 1) SELECT * FROM temp")
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 1 AND p2 = 1"), df)

          // test can delete data correctly
          sql("INSERT INTO TABLE t PARTITION (p1 = 1, p2 = 1) SELECT * FROM temp")
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 1 AND p2 = 1"),
            Row(1) :: Row(2) :: Row(3) :: Row(1) :: Row(2) :: Row(3) :: Nil)

          // customized partition location
          withTempPath { path =>
            sql(
              s"""
                 |ALTER TABLE t ADD PARTITION (p1=3, p2=3)
                 |LOCATION '$path'
                 |""".stripMargin)
            sql("INSERT OVERWRITE TABLE t PARTITION (p1 = 3, p2 = 3) SELECT 3")
            checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 3 AND p2 = 3"), Row(3) :: Nil)
            sql("INSERT INTO TABLE t PARTITION (p1 = 3, p2 = 3) SELECT 3")
            checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 3 AND p2 = 3"), Row(3) :: Row(3) :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-36571: Dynamic partition overwrite - DYNAMIC mode") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SQLOverwriteHadoopMapReduceCommitProtocol].getName,
      SQLConf.PARTITION_OVERWRITE_MODE.key -> DYNAMIC.toString) {
      withTable("t") {
        withTempView("temp") {
          sql(
            s"""
               | CREATE TABLE t(c1 int, p1 string, p2 string)
               | USING PARQUET
               | PARTITIONED BY (p1, p2)
            """.stripMargin)

          val df = Seq((1, "1", "1"), (2, "2", "2"), (3, "3", "3")).toDF("c1", "p1", "p2")
          df.createOrReplaceTempView("temp")
          sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"), df)
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 1 AND p2 = 1"), Row(1) :: Nil)

          Seq((3, 3, 3), (4, 4, 4), (5, 5, 5))
            .toDF("c1", "p1", "p2").createOrReplaceTempView("temp")
          // test won't delete other partitions data
          sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"),
            Row(1, "1", "1") :: Row(2, "2", "2") :: Row(3, "3", "3") ::
              Row(4, "4", "4") :: Row(5, "5", "5") :: Nil)
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 5 AND p2 = 5"), Row(5) :: Nil)

          // customized partition location
          withTempPath { path =>
            sql(
              s"""
                 |ALTER TABLE t ADD PARTITION (p1=6, p2=6)
                 |LOCATION '$path'
                 |""".stripMargin)
            Seq((5, 5, 5), (6, 6, 6))
              .toDF("c1", "p1", "p2").createOrReplaceTempView("temp")
            sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
            checkAnswer(sql("SELECT * FROM t"),
              Row(1, "1", "1") :: Row(2, "2", "2") :: Row(3, "3", "3") ::
                Row(4, "4", "4") :: Row(5, "5", "5") :: Row(6, "6", "6") :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-36571: Dynamic partition overwrite - STATIC mode") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SQLOverwriteHadoopMapReduceCommitProtocol].getName,
      SQLConf.PARTITION_OVERWRITE_MODE.key -> STATIC.toString) {
      withTable("t") {
        withTempView("temp") {
          sql(
            s"""
               | CREATE TABLE t(c1 int, p1 string, p2 string)
               | USING PARQUET
               | PARTITIONED BY (p1, p2)
            """.stripMargin)

          val df = Seq((1, "1", "1"), (2, "2", "2"), (3, "3", "3")).toDF("c1", "p1", "p2")
          df.createOrReplaceTempView("temp")
          sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"), df)
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 1 AND p2 = 1"), Row(1) :: Nil)

          Seq((3, 3, 3), (4, 4, 4), (5, 5, 5))
            .toDF("c1", "p1", "p2").createOrReplaceTempView("temp")
          // test won't delete other partitions data
          sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
          checkAnswer(sql("SELECT * FROM t"),
            Row(3, "3", "3") :: Row(4, "4", "4") :: Row(5, "5", "5") :: Nil)
          checkAnswer(sql("SELECT c1 FROM t WHERE p1 = 5 AND p2 = 5"), Row(5) :: Nil)

          // customized partition location
          withTempPath { path =>
            sql(
              s"""
                 |ALTER TABLE t ADD PARTITION (p1=6, p2=6)
                 |LOCATION '$path'
                 |""".stripMargin)
            Seq((5, 5, 5), (6, 6, 6))
              .toDF("c1", "p1", "p2").createOrReplaceTempView("temp")
            sql("INSERT OVERWRITE TABLE t SELECT * FROM temp")
            checkAnswer(sql("SELECT * FROM t"), Row(5, "5", "5") :: Row(6, "6", "6") :: Nil)
          }
        }
      }
    }
  }
}
