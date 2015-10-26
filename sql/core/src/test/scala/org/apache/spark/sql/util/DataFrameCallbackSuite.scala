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

package org.apache.spark.sql.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.sql.{functions, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameCallbackSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  import functions._

  test("execute callback functions when a DataFrame action finished successfully") {
    val metrics = ArrayBuffer.empty[(String, QueryExecution, Long)]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += ((funcName, qe, duration))
      }
    }
    sqlContext.listenerManager.register(listener)

    val df = Seq(1 -> "a").toDF("i", "j")
    df.select("i").collect()
    df.filter($"i" > 0).count()

    assert(metrics.length == 2)

    assert(metrics(0)._1 == "collect")
    assert(metrics(0)._2.analyzed.isInstanceOf[Project])
    assert(metrics(0)._3 > 0)

    assert(metrics(1)._1 == "count")
    assert(metrics(1)._2.analyzed.isInstanceOf[Aggregate])
    assert(metrics(1)._3 > 0)

    sqlContext.listenerManager.unregister(listener)
  }

  test("execute callback functions when a DataFrame action failed") {
    val metrics = ArrayBuffer.empty[(String, QueryExecution, Exception)]
    val listener = new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        metrics += ((funcName, qe, exception))
      }

      // Only test failed case here, so no need to implement `onSuccess`
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {}
    }
    sqlContext.listenerManager.register(listener)

    val errorUdf = udf[Int, Int] { _ => throw new RuntimeException("udf error") }
    val df = sparkContext.makeRDD(Seq(1 -> "a")).toDF("i", "j")

    // Ignore the log when we are expecting an exception.
    sparkContext.setLogLevel("FATAL")
    val e = intercept[SparkException](df.select(errorUdf($"i")).collect())

    assert(metrics.length == 1)
    assert(metrics(0)._1 == "collect")
    assert(metrics(0)._2.analyzed.isInstanceOf[Project])
    assert(metrics(0)._3.getMessage == e.getMessage)

    sqlContext.listenerManager.unregister(listener)
  }

  test("get numRows metrics by callback") {
    val metrics = ArrayBuffer.empty[Long]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += qe.executedPlan.longMetric("numInputRows").value.value
      }
    }
    sqlContext.listenerManager.register(listener)

    val df = Seq(1 -> "a").toDF("i", "j").groupBy("i").count()
    df.collect()
    df.collect()
    Seq(1 -> "a", 2 -> "a").toDF("i", "j").groupBy("i").count().collect()

    assert(metrics.length == 3)
    assert(metrics(0) == 1)
    assert(metrics(1) == 1)
    assert(metrics(2) == 2)

    sqlContext.listenerManager.unregister(listener)
  }

  // TODO: Currently some LongSQLMetric use -1 as initial value, so if the accumulator is never
  // updated, we can filter it out later.  However, when we aggregate(sum) accumulator values at
  // driver side for SQL physical operators, these -1 values will make our result smaller.
  // A easy fix is to create a new SQLMetric(including new MetricValue, MetricParam, etc.), but we
  // can do it later because the impact is just too small (1048576 tasks for 1 MB).
  ignore("get size metrics by callback") {
    val metrics = ArrayBuffer.empty[Long]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += qe.executedPlan.longMetric("dataSize").value.value
        val bottomAgg = qe.executedPlan.children(0).children(0)
        metrics += bottomAgg.longMetric("dataSize").value.value
      }
    }
    sqlContext.listenerManager.register(listener)

    val sparkListener = new SaveInfoListener
    sqlContext.sparkContext.addSparkListener(sparkListener)

    val df = (1 to 100).map(i => i -> i.toString).toDF("i", "j")
    df.groupBy("i").count().collect()

    def getPeakExecutionMemory(stageId: Int): Long = {
      val peakMemoryAccumulator = sparkListener.getCompletedStageInfos(stageId).accumulables
        .filter(_._2.name == InternalAccumulator.PEAK_EXECUTION_MEMORY)

      assert(peakMemoryAccumulator.size == 1)
      peakMemoryAccumulator.head._2.value.toLong
    }

    assert(sparkListener.getCompletedStageInfos.length == 2)
    val bottomAggDataSize = getPeakExecutionMemory(0)
    val topAggDataSize = getPeakExecutionMemory(1)

    // For this simple case, the peakExecutionMemory of a stage should be the data size of the
    // aggregate operator, as we only have one memory consuming operator per stage.
    assert(metrics.length == 2)
    assert(metrics(0) == topAggDataSize)
    assert(metrics(1) == bottomAggDataSize)

    sqlContext.listenerManager.unregister(listener)
  }
}
