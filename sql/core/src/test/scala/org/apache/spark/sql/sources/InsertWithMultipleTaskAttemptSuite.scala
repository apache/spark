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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.SharedSparkSession

class InsertWithMultipleTaskAttemptSuite extends QueryTest with SharedSparkSession {
  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(config.MAX_LOCAL_TASK_FAILURES, 3)
  }

  test("it is allowed to insert into a table for dynamic partition overwrite " +
    "while speculation on") {
    withSQLConf(
      SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key -> classOf[InsertExceptionCommitProtocol].getName) {
      withTable("insertTable") {
        sql(
          """
            |CREATE TABLE insertTable(i int, part1 int, part2 int) USING PARQUET
            |PARTITIONED BY (part1, part2)
          """.stripMargin)

        sql(
          """
            |INSERT OVERWRITE TABLE insertTable Partition(part1=1, part2)
            |SELECT 1,2
          """.stripMargin)
        checkAnswer(spark.table("insertTable"), Row(1, 1, 2))
      }
    }
  }
}

class InsertExceptionCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {
  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage =
    if (InsertExceptionCommitProtocol.inited) {
      throw new Exception("test")
    } else {
      super.commitTask(taskContext)
    }
}

object InsertExceptionCommitProtocol {
  private val initedFlag = new AtomicBoolean(false)
  def inited: Boolean = initedFlag.compareAndSet(false, true)
}
