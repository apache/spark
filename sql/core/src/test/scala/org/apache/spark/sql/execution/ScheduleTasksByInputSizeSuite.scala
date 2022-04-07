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

package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.internal.config
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class ScheduleTasksByInputSizeSuiteBase extends QueryTest with SharedSparkSession {

  protected def sortTasksByInputSizeEnabled: Boolean = false
  private val tempDir = Utils.createTempDir()

  protected override def sparkConf = {
    super.sparkConf
      .set("spark.default.parallelism", "1")
      .set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, "1")
      .set(SQLConf.FILES_MAX_PARTITION_BYTES.key, "1200")
      .set(config.SCHEDULER_SORT_TASKS_BY_INPUT_SIZE.key, sortTasksByInputSizeEnabled.toString)
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    Seq(
      spark.range(1),
      spark.range(100),
      spark.range(10),
      spark.range(1000)
    ).foreach { df =>
      df.write
        .format("parquet")
        .mode("append")
        .save(tempDir.getCanonicalPath)
    }
  }

  protected override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  private def findFileScanRDD(df: DataFrame): FileScanRDD = {
    var rdd = df.rdd
    while (!rdd.isInstanceOf[FileScanRDD]) {
      rdd = rdd.firstParent
    }
    assert(rdd.isInstanceOf[FileScanRDD])
    rdd.asInstanceOf[FileScanRDD]
  }

  private def findShuffledRowRDD(df: DataFrame): ShuffledRowRDD = {
    var rdd = df.rdd
    while (!rdd.isInstanceOf[ShuffledRowRDD]) {
      rdd = rdd.firstParent
    }
    assert(rdd.isInstanceOf[ShuffledRowRDD])
    rdd.asInstanceOf[ShuffledRowRDD]
  }

  private def doWithCollectTasks(df: DataFrame) : Array[Int] = {
    val scheduled = new ArrayBuffer[Int]
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        scheduled.append(taskStart.taskInfo.partitionId)
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      df.collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    scheduled.toArray
  }

  private def checkTasksStarOrdering(
      scheduled: Array[Int], parts: Array[Partition]): Unit = {
    val input = parts.map(_.index)
    if (sortTasksByInputSizeEnabled) {
      assert(input.zip(scheduled).exists { case (l, r) => l != r})
      val expected = parts.sortBy(_.inputSize.get)(Ordering[Long].reverse)
        .map(_.index)
      assert(expected.zip(scheduled).forall { case (l, r) => l == r})
    } else {
      assert(input.zip(scheduled).forall { case (l, r) => l == r})
    }
  }

  test("Support datasource v1 file scan") {
    val df = spark.read.parquet(tempDir.getCanonicalPath)
    val scheduled = doWithCollectTasks(df)
    val fileScanRDDParts = findFileScanRDD(df).partitions

    // 1200
    // 1200
    // 1200
    // 885
    // 885
    // 991 (519,472)
    assert(scheduled.length == 6)
    assert(fileScanRDDParts.length == 6)
    assert(fileScanRDDParts.forall(_.inputSize.isDefined))
    checkTasksStarOrdering(scheduled, fileScanRDDParts)
  }

  test("Support coalesce partition spec") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "3300") {
      val df = spark.read.parquet(tempDir.getCanonicalPath).distinct()
      val totalScheduled = doWithCollectTasks(df)
      // skip file scan stage
      val scheduled = totalScheduled.slice(6, totalScheduled.length)
      val shuffledRowRDDParts = findShuffledRowRDD(df).partitions

      // 1914
      // 3108
      // 1716
      // 1716
      assert(shuffledRowRDDParts.length == 4)
      assert(scheduled.length == shuffledRowRDDParts.length)
      checkTasksStarOrdering(scheduled, shuffledRowRDDParts)
    }
  }
}

class ScheduleTasksByInputSizeSuiteEnabled extends ScheduleTasksByInputSizeSuiteBase {
  override protected val sortTasksByInputSizeEnabled: Boolean = true
}

class ScheduleTasksByInputSizeSuiteDisabled extends ScheduleTasksByInputSizeSuiteBase {
  override protected val sortTasksByInputSizeEnabled: Boolean = false
}
