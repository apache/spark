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

package org.apache.spark.examples.streaming

import com.twitter.algebird._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

// scalastyle:off
/**
 * Illustrates the use of the Count-Min Sketch, from Twitter's Algebird library, to compute
 * windowed and global Top-K estimates of user IDs occurring in a Twitter stream.
 * <br>
 *   <strong>Note</strong> that since Algebird's implementation currently only supports Long inputs,
 *   the example operates on Long IDs. Once the implementation supports other inputs (such as String),
 *   the same approach could be used for computing popular topics for example.
 * <p>
 * <p>
 *   <a href=
 *   "http://highlyscalable.wordpress.com/2012/05/01/probabilistic-structures-web-analytics-data-mining/">
 *   This blog post</a> has a good overview of the Count-Min Sketch (CMS). The CMS is a data
 *   structure for approximate frequency estimation in data streams (e.g. Top-K elements, frequency
 *   of any given element, etc), that uses space sub-linear in the number of elements in the
 *   stream. Once elements are added to the CMS, the estimated count of an element can be computed,
 *   as well as "heavy-hitters" that occur more than a threshold percentage of the overall total
 *   count.
 * <p><p>
 *   Algebird's implementation is a monoid, so we can succinctly merge two CMS instances in the
 *   reduce operation.
 */
// scalastyle:on
object TwitterAlgebirdCMS {
  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()

    // CMS parameters
    val DELTA = 1E-3
    val EPS = 0.01
    val SEED = 1
    val PERC = 0.001
    // K highest frequency elements to take
    val TOPK = 10

    val filters = args
    val sparkConf = new SparkConf().setAppName("TwitterAlgebirdCMS")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER_2)

    val users = stream.map(status => status.getUser.getId)

    val cms = new CountMinSketchMonoid(EPS, DELTA, SEED, PERC)
    var globalCMS = cms.zero
    val mm = new MapMonoid[Long, Int]()
    var globalExact = Map[Long, Int]()

    val approxTopUsers = users.mapPartitions(ids => {
      ids.map(id => cms.create(id))
    }).reduce(_ ++ _)

    val exactTopUsers = users.map(id => (id, 1))
      .reduceByKey((a, b) => a + b)

    approxTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        val partialTopK = partial.heavyHitters.map(id =>
          (id, partial.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        globalCMS ++= partial
        val globalTopK = globalCMS.heavyHitters.map(id =>
          (id, globalCMS.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        println("Approx heavy hitters at %2.2f%% threshold this batch: %s".format(PERC,
          partialTopK.mkString("[", ",", "]")))
        println("Approx heavy hitters at %2.2f%% threshold overall: %s".format(PERC,
          globalTopK.mkString("[", ",", "]")))
      }
    })

    exactTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partialMap = rdd.collect().toMap
        val partialTopK = rdd.map(
          {case (id, count) => (count, id)})
          .sortByKey(ascending = false).take(TOPK)
        globalExact = mm.plus(globalExact.toMap, partialMap)
        val globalTopK = globalExact.toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        println("Exact heavy hitters this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("Exact heavy hitters overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
