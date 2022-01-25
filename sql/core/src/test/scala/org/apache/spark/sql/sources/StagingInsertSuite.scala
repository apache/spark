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

package org.apache.spark.sql.sources

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.{SQLHadoopMapReduceCommitProtocol, SQLPathHadoopMapReduceCommitProtocol}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class StagingInsertSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  val stagingParentDir = Utils.createTempDir()

  val stagingDir = stagingParentDir.getCanonicalPath + "/.spark-stagingDir"

  override def beforeAll(): Unit = {
    super.beforeAll()
    stagingParentDir.delete()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(stagingParentDir)
    } finally {
      super.afterAll()
    }
  }

  test("SPDI-25701: Assert staging dir work") {
    withSQLConf(SQLConf.EXEC_STAGING_DIR.key -> stagingDir) {
      val commitProtocol = new SQLHadoopMapReduceCommitProtocol("job-id", "dummy-path", true)
      assert(commitProtocol.stagingDir.toString.contains(stagingDir))
    }
  }

  test("SPARK-36579: dynamic partition overwrite can use user defined staging dir") {
    // Partition Insert
    Seq("static", "dynamic").foreach { mode =>
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> mode,
        SQLConf.EXEC_STAGING_DIR.key -> stagingDir) {
        withTempDir { d =>
          withTable("t") {
            sql(
              s"""
                 |CREATE TABLE t(c1 int, p1 int) USING PARQUET PARTITIONED BY(p1)
                 |LOCATION '${d.getAbsolutePath}'
             """.stripMargin)

            val df = Seq((1, 2), (3, 4), (5, 6)).toDF("c1", "p1")
            df.write
              .partitionBy("p1")
              .mode("overwrite")
              .saveAsTable("t")
            checkAnswer(sql("SELECT * FROM t"), df)
            checkAnswer(sql("SELECT * FROM t WHERE p1 = 2"), Row(1, 2) :: Nil)
            checkAnswer(sql("SELECT * FROM t WHERE p1 = 4"), Row(3, 4) :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-36571: Add a new commit protocol - None-partitioned insert") {
    Seq("1", "2").foreach { version =>
      withSQLConf(SQLConf.EXEC_STAGING_DIR.key -> stagingDir,
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> version,
        SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          classOf[SQLPathHadoopMapReduceCommitProtocol].getName) {
        withTable("t") {
          sql(
            s"""
               | CREATE TABLE t(c1 int, p1 int) using PARQUET
            """.stripMargin)

          val df =
            Seq((1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2))
              .toDF("c1", "p1").repartition(1)
          df.write
            .mode("overwrite")
            .format("parquet")
            .saveAsTable("t")
          checkAnswer(sql("SELECT * FROM t"), df)
        }
      }
    }
  }

  test("SPARK-36571: Add a new commit protocol - Static partition insert") {
    Seq("1", "2").foreach { version =>
      withSQLConf(SQLConf.EXEC_STAGING_DIR.key -> stagingDir,
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> version,
        SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          classOf[SQLPathHadoopMapReduceCommitProtocol].getName) {
        withTempDir { d =>
          withTable("t") {
            sql(
              s"""
                 | CREATE TABLE t(c1 int, p1 int) USING PARQUET PARTITIONED BY(p1)
                 | LOCATION '${d.getAbsolutePath}'
            """.stripMargin)

            val df =
              Seq((1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2))
                .toDF("c1", "p1").repartition(1)
            df.createOrReplaceTempView("view1")
            sql("INSERT OVERWRITE t PARTITION(p1=2) SELECT c1 FROM view1")
            checkAnswer(sql("SELECT * FROM t WHERE p1=2"), df)
          }
        }
      }
    }
  }

  test("SPARK-36571: Add a new commit protocol - Dynamic partition insert") {
    Seq("1", "2").foreach { version =>
      withSQLConf(SQLConf.EXEC_STAGING_DIR.key -> stagingDir,
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> version,
        SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          classOf[SQLPathHadoopMapReduceCommitProtocol].getName) {
        withTable("t") {
          sql(
            s"""
               | CREATE TABLE t(c1 int, p1 int) USING PARQUET PARTITIONED BY(p1)
            """.stripMargin)

          val df = Seq((1, 2), (3, 4), (5, 6), (7, 8)).toDF("c1", "p1")
          df.write
            .partitionBy("p1")
            .mode("overwrite")
            .saveAsTable("t")
          checkAnswer(sql("SELECT * FROM t"), df)
          checkAnswer(sql("SELECT * FROM t WHERE p1 > 5"), Row(5, 6) :: Row(7, 8) :: Nil)
          checkAnswer(sql("SELECT * FROM t WHERE p1 = 4"), Row(3, 4) :: Nil)
        }
      }
    }
  }
}
