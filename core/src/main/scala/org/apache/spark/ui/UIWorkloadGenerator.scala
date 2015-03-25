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

package org.apache.spark.ui

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.SchedulingMode

// scalastyle:off
/**
 * Continuously generates jobs that expose various features of the WebUI (internal testing tool).
 *
 * Usage: ./bin/spark-class org.apache.spark.ui.UIWorkloadGenerator [master] [FIFO|FAIR] [#job set (4 jobs per set)]
 */
// scalastyle:on
private[spark] object UIWorkloadGenerator {

  val NUM_PARTITIONS = 100
  val INTER_JOB_WAIT_MS = 5000

  def main(args: Array[String]) {
    if (args.length < 3) {
      println(
        "usage: ./bin/spark-class org.apache.spark.ui.UIWorkloadGenerator " +
          "[master] [FIFO|FAIR] [#job set (4 jobs per set)]")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster(args(0)).setAppName("Spark UI tester")

    val schedulingMode = SchedulingMode.withName(args(1))
    if (schedulingMode == SchedulingMode.FAIR) {
      conf.set("spark.scheduler.mode", "FAIR")
    }
    val nJobSet = args(2).toInt
    val sc = new SparkContext(conf)

    def setProperties(s: String): Unit = {
      if(schedulingMode == SchedulingMode.FAIR) {
        sc.setLocalProperty("spark.scheduler.pool", s)
      }
      sc.setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, s)
    }

    val baseData = sc.makeRDD(1 to NUM_PARTITIONS * 10, NUM_PARTITIONS)
    def nextFloat(): Float = new Random().nextFloat()

    val jobs = Seq[(String, () => Long)](
      ("Count", baseData.count),
      ("Cache and Count", baseData.map(x => x).cache().count),
      ("Single Shuffle", baseData.map(x => (x % 10, x)).reduceByKey(_ + _).count),
      ("Entirely failed phase", baseData.map(x => throw new Exception).count),
      ("Partially failed phase", {
        baseData.map{x =>
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            throw new Exception("This is a task failure")
          }
          1
        }.count
      }),
      ("Partially failed phase (longer tasks)", {
        baseData.map{x =>
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            Thread.sleep(100)
            throw new Exception("This is a task failure")
          }
          1
        }.count
      }),
      ("Job with delays", baseData.map(x => Thread.sleep(100)).count)
    )

    (1 to nJobSet).foreach { _ =>
      for ((desc, job) <- jobs) {
        new Thread {
          override def run() {
            try {
              setProperties(desc)
              job()
              println("Job funished: " + desc)
            } catch {
              case e: Exception =>
                println("Job Failed: " + desc)
            }
          }
        }.start
        Thread.sleep(INTER_JOB_WAIT_MS)
      }
    }
    sc.stop()
  }
}
