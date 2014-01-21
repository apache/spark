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

package org.apache.spark.streaming.examples

import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HyperLogLog._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

/**
 * Illustrates the use of the HyperLogLog algorithm, from Twitter's Algebird library, to compute
 * a windowed and global estimate of the unique user IDs occurring in a Twitter stream.
 * <p>
 * <p>
 *   This <a href="http://highlyscalable.wordpress.com/2012/05/01/probabilistic-structures-web-analytics-data-mining/">
 *   blog post</a> and this
 *   <a href="http://highscalability.com/blog/2012/4/5/big-data-counting-how-to-count-a-billion-distinct-objects-us.html">blog post</a>
 *   have good overviews of HyperLogLog (HLL). HLL is a memory-efficient datastructure for estimating
 *   the cardinality of a data stream, i.e. the number of unique elements.
 * <p><p>
 *   Algebird's implementation is a monoid, so we can succinctly merge two HLL instances in the reduce operation.
 */
object TwitterAlgebirdHLL {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterAlgebirdHLL <master>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    /** Bit size parameter for HyperLogLog, trades off accuracy vs size */
    val BIT_SIZE = 12
    val (master, filters) = (args.head, args.tail)

    val ssc = new StreamingContext(master, "TwitterAlgebirdHLL", Seconds(5),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
    val stream = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER)

    val users = stream.map(status => status.getUser.getId)

    val hll = new HyperLogLogMonoid(BIT_SIZE)
    var globalHll = hll.zero
    var userSet: Set[Long] = Set()

    val approxUsers = users.mapPartitions(ids => {
      ids.map(id => hll(id))
    }).reduce(_ + _)

    val exactUsers = users.map(id => Set(id)).reduce(_ ++ _)

    approxUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        globalHll += partial
        println("Approx distinct users this batch: %d".format(partial.estimatedSize.toInt))
        println("Approx distinct users overall: %d".format(globalHll.estimatedSize.toInt))
      }
    })

    exactUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        userSet ++= partial
        println("Exact distinct users this batch: %d".format(partial.size))
        println("Exact distinct users overall: %d".format(userSet.size))
        println("Error rate: %2.5f%%".format(((globalHll.estimatedSize / userSet.size.toDouble) - 1) * 100))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
