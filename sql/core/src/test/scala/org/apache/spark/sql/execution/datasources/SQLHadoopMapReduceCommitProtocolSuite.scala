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

  override def abortTask(context: TaskAttemptContext): Unit = {
    sys.error("mock abortTask failed")
  }
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

  test("SPARK-32395: Commit output files after task attempt succeed") {
    val commitClazz = classOf[UserParquetOutputCommitter].getName
    sparkContext.hadoopConfiguration.set(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key, commitClazz)

    withTable("insertTable") {
      sql(
        """
          |CREATE TABLE insertTable(i int, part int) USING PARQUET
          |PARTITIONED BY (part)
          |OPTIONS (
          |  `partitionOverwriteMode` 'dynamic',
          |  `serialization.format` '1'
          |)
          """.stripMargin)

      sql(
        s"""INSERT OVERWRITE TABLE insertTable PARTITION (part)
           |SELECT /*+REPARTITION(2) */ id, 1
           |FROM range(10)
           |""".stripMargin)
      val expected = Range(0, 10).map(Row(_, 1))
      checkAnswer(spark.table("insertTable"), expected)
    }
  }
}
