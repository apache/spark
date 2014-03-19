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

package main.scala

import scala.collection.mutable.{ListBuffer, Queue}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._

object SparkStreamingExample {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .setMaster("local[2]")
      .setAppName("Streaming test")
    val ssc = new StreamingContext(conf, Seconds(1))
    val seen = ListBuffer[RDD[Int]]()

    val rdd1 = ssc.sparkContext.makeRDD(1 to 100, 10)
    val rdd2 = ssc.sparkContext.makeRDD(1 to 1000, 10)
    val rdd3 = ssc.sparkContext.makeRDD(1 to 10000, 10)

    val queue = Queue(rdd1, rdd2, rdd3)
    val stream = ssc.queueStream(queue)

    stream.foreachRDD(rdd => seen += rdd)
    ssc.start()
    Thread.sleep(5000)

    def test(f: => Boolean, failureMsg: String) = {
      if (!f) {
        println(failureMsg)
        System.exit(-1)
      }
    }

    val rddCounts = seen.map(rdd => rdd.count()).filter(_ > 0)
    test(rddCounts.length == 3, "Did not collect three RDD's from stream")
    test(rddCounts.toSet == Set(100, 1000, 10000), "Did not find expected streams")

    println("Test succeeded")

    ssc.stop()
  }
}
