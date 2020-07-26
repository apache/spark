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
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.SparkContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.TestSparkSession

class UserParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitTask(context: TaskAttemptContext): Unit = {
    if (context.getTaskAttemptID.getId == 1) {
      sys.error("mock commitTask failed")
    }
  }
}

class SQLHadoopMapReduceCommitProtocolSuite extends QueryTest with SharedSparkSession {

  override def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(new SparkContext("local[2,2]", "test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))
  }

  val nonPartitionTableName: String = "nonPartTable"

  val partitionTableName: String = "partTable"

  def createNonPartitionTable(): Unit = {
    sql(
      s"""
         |CREATE TABLE $nonPartitionTableName(i int) USING PARQUET
         |OPTIONS (
         |  `partitionOverwriteMode` 'dynamic',
         |  `serialization.format` '1'
         |)
          """.stripMargin)
  }

  def createPartitionTable(): Unit = {
    sql(
      s"""
         |CREATE TABLE $partitionTableName(i int, part int) USING PARQUET
         |PARTITIONED BY (part)
         |OPTIONS (
         |  `partitionOverwriteMode` 'dynamic',
         |  `serialization.format` '1'
         |)
          """.stripMargin)
  }

  def testWithNonPartitionTable(body: () => Unit): Unit = {
    withTable(nonPartitionTableName) {
      createNonPartitionTable()
      body
    }
  }

  def testWithPartitionTable[T](body: => T): Unit = {
    withTable(partitionTableName) {
      createPartitionTable()
      body
    }
  }

  test("write partition table") {
    val commitClazz = classOf[UserParquetOutputCommitter].getName
    sparkContext.hadoopConfiguration.set(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key, commitClazz)

    testWithPartitionTable {
      val expected = Range(0, 10).map(Row(_, 1))

      sql(
        s"""INSERT OVERWRITE TABLE $partitionTableName PARTITION (part=1)
           |SELECT id
           |FROM range(5)
           |""".stripMargin)

      sql(
        s"""INSERT OVERWRITE TABLE $partitionTableName PARTITION (part=1)
           |SELECT /*+ REPARTITION(2) */ id
           |FROM range(10)
           |""".stripMargin)
      checkAnswer(spark.table(partitionTableName), expected)
    }
  }

  test("write dynamic partition table") {
    val commitClazz = classOf[UserParquetOutputCommitter].getName
    sparkContext.hadoopConfiguration.set(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key, commitClazz)

    testWithPartitionTable {
      val expected = Range(0, 10).flatMap(k => Seq(Row(k, 1), Row(k, 2)))

      sql(
        s"""INSERT OVERWRITE TABLE $partitionTableName PARTITION (part)
           |SELECT id, 1
           |FROM range(5)
           |""".stripMargin)

      sql(
        s"""INSERT OVERWRITE TABLE $partitionTableName PARTITION (part)
           |SELECT /*+ REPARTITION(2) */ id, 1
           |FROM range(10)
           |UNION ALL
           |SELECT /*+ REPARTITION(2) */ id, 2
           |FROM range(10)
           |""".stripMargin)
      checkAnswer(spark.table(partitionTableName), expected)
    }
  }
}
