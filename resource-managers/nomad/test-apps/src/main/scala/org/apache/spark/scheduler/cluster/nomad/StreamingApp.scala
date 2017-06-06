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

package org.apache.spark.scheduler.cluster.nomad

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

private[spark] object StreamingApp extends TestApplication {

  def main(args: Array[String]) {
    checkArgs(args)("result_url")
    val Array(resultUrl) = args

    val sparkConf = new SparkConf().setAppName("QueueStream")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    try {
      httpPut(resultUrl) {

        // Create the queue through which RDDs can be pushed to
        // a QueueInputDStream
        val rddQueue = new mutable.Queue[RDD[Int]]()

        // Create the QueueInputDStream and use it do some processing
        val count = new AtomicInteger()
        ssc.queueStream(rddQueue)
          .map(x => (x % 10, 1))
          .reduceByKey(_ + _)
          .foreachRDD(_ => count.incrementAndGet())

        ssc.start()

        // Create and push some RDDs into rddQueue
        for (i <- 1 to 30) {
          rddQueue.synchronized {
            rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
          }
          Thread.sleep(1000)
        }

        val finalCount = count.get
        if (finalCount >= 30) "success"
        else finalCount.toString
      }
    } finally ssc.stop()
  }
}
