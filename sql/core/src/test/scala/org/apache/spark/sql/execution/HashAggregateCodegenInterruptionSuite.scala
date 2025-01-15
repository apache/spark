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

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.logging.log4j.Level

import org.apache.spark.{SparkConf, TaskEndReason, TaskKilled}
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class HashAggregateCodegenInterruptionSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super
      .sparkConf
      .set("spark.task.reaper.enabled", "true")
      .set("spark.task.reaper.killTimeout", "10s")
  }

  test("SPARK-50806: HashAggregate codegen should be interrupted on task cancellation") {
    import testImplicits._
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> true.toString,
      SQLConf.INTERRUPT_ON_CANCEL.key -> false.toString) {
      var taskId = -1L
      var isJobEnded = false
      var taskEndReasons = new mutable.ArrayBuffer[TaskEndReason]
      spark.sparkContext.addSparkListener(new org.apache.spark.scheduler.SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          taskId = taskStart.taskInfo.taskId
        }

        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          taskEndReasons += taskEnd.reason
        }

        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          isJobEnded = true
        }
      })

      val logAppender = new LogAppender("")
      withLogAppender(logAppender, level = Some(Level.INFO)) {
        spark.sparkContext.setJobGroup("SPARK-50806", "SPARK-50806", false)
        // The dataset is set to 100k as we are monitoring interruptions for every 1k rows. Two
        // tasks (50 seconds each, totaling 100k / 2) exceed `spark.task.reaper.killTimeout` (10s),
        // which should provide a proper test for the interruption behavior.
        val slowDF = spark.range(1, 100000).rdd.mapPartitions { iter =>
          new Iterator[Long] {
            var cnt = 0
            override def hasNext: Boolean = iter.hasNext
            override def next(): Long = {
              if (cnt % 1000 == 0) {
                Thread.sleep(1000)
              }
              cnt += 1
              iter.next()
            }
          }
        }.toDF("id")
        val aggDF = slowDF.selectExpr("id % 10 as key").groupBy("key").agg(sum("key"))
        import scala.concurrent.ExecutionContext.global
        Future {
          aggDF.collect()
        }(global)
        // Leave some time for the query to start running
        Thread.sleep(5000)
        spark.sparkContext.cancelJobGroup("SPARK-50806")
        eventually(timeout(1.minute)) {
          assert(isJobEnded)
          assert(taskEndReasons.length  === 2)
          assert(taskEndReasons.forall(_.isInstanceOf[TaskKilled]))
          val logs = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
          assert(!logs.exists(
            _.contains(s"Killed task $taskId could not be stopped within 10000 ms"))
          )
        }
      }
    }
  }
}
