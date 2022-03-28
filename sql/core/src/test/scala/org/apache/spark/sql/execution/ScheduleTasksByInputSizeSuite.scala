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

import org.apache.spark.internal.config
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class ScheduleTasksByInputSizeSuiteBase extends QueryTest with SharedSparkSession {
  protected val sortTasksByInputSizeEnabled: Boolean = false
  private val tempDir = Utils.createTempDir()

  protected override def sparkConf = {
    super.sparkConf
      .set("spark.default.parallelism", "1")
      .set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, "128M")
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

  test("Support datasource v1 file scan") {
    val tasks = new ArrayBuffer[Int]
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        // note that, taskInfo.index is not always same with partition index
        tasks.append(taskStart.taskInfo.index)
      }
    }
    var fileScanRDD: FileScanRDD = null
    spark.sparkContext.addSparkListener(listener)
    try {
      val df = spark.read.parquet(tempDir.getCanonicalPath)
      df.collect()
      fileScanRDD = findFileScanRDD(df)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(tasks.size == 5)
    assert(fileScanRDD.partitions.length == 4)
    assert(fileScanRDD.partitions.forall(_.inputSize.isDefined))

    // SPARK-37831: here should be partition index
    val input = fileScanRDD.partitions.map(_.index)
    // skip the first task which is not related file scan
    val scheduled = tasks.tail
    assert(input.zip(scheduled).exists { case (l, r) => l != r})

    val expected = fileScanRDD.partitions.sortBy(_.inputSize.get).reverse.map(_.index)
    assert(expected.zip(scheduled).forall { case (l, r) => l == r} == sortTasksByInputSizeEnabled)
  }
}

class ScheduleTasksByInputSizeSuiteEnabled extends ScheduleTasksByInputSizeSuiteBase {
  override protected val sortTasksByInputSizeEnabled: Boolean = true
}


class ScheduleTasksByInputSizeSuiteDisabled extends ScheduleTasksByInputSizeSuiteBase {
  override protected val sortTasksByInputSizeEnabled: Boolean = false
}
