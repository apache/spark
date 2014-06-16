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

package org.apache.spark.examples

import java.io.{PrintWriter, FileOutputStream}
import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object IndexedRDDBenchmark {

  def main(args: Array[String]) {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    var numPartitions = 1000
    var elemsPerPartition = 1000000
    var trials = 100

    options.foreach {
      case ("numPartitions", v) => numPartitions = v.toInt
      case ("elemsPerPartition", v) => elemsPerPartition = v.toInt
      case ("trials", v) => trials = v.toInt
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    val numElements = numPartitions * elemsPerPartition

    val conf = new SparkConf()
      .setAppName(s"IndexedRDD Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val r = new util.Random(0)

    println("Constructing vanilla RDD...")
    val vanilla = sc.parallelize(0 until numPartitions, numPartitions).flatMap(p =>
      (p * elemsPerPartition) until ((p + 1) * elemsPerPartition))
      .map(x => (x.toLong, x)).cache()
    println("Constructing indexed RDD...")
    val indexed = IndexedRDD(vanilla).cache()
    println(s"Done. Generated ${vanilla.count}, ${indexed.count} elements.")

    println(s"Scanning vanilla RDD with mapValues ($trials trials)...")
    var start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val doubled = vanilla.mapValues(_ * 2)
      doubled.foreach(x => {})
    }
    var end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per scan.")
    println(s"Scanning indexed RDD with mapValues ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val doubled = indexed.mapValues(_ * 2)
      doubled.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per scan.")

    println("Constructing modified version of vanilla RDD...")
    val vanilla2 = vanilla.mapValues(_ * 2).cache()
    vanilla2.foreach(x => {})
    println("Constructing modified version of indexed RDD...")
    val indexed2 = indexed.mapValues(_ * 2).cache()
    indexed2.foreach(x => {})
    println(s"Done.")

    println(s"Zipping vanilla RDD with modified version ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val zipped = vanilla.zip(vanilla2).map(ab => (ab._1._1, ab._1._2 + ab._2._2))
      zipped.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per zip.")
    // println(s"Joining vanilla RDD with modified version ($trials trials)...")
    // start = System.currentTimeMillis
    // for (i <- 1 to trials) {
    //   val joined = vanilla.join(vanilla2)
    //   joined.foreach(x => {})
    // }
    // end = System.currentTimeMillis
    // println(s"Done. ${(end - start) / trials} ms per join.")
    println(s"Joining indexed RDD with modified version ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val joined = indexed.innerJoin(indexed2) { (id, a, b) => a + b }
      joined.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per join.")


    // println(s"Get on vanilla RDD ($trials trials)...")
    // var start = System.currentTimeMillis
    // for (i <- 1 to trials) {
    //   val elem = r.nextInt(numElements)
    //   val value = large.get(elem)
    //   assert(value == Some(elem), s"get($elem) was $value")
    // }
    // var end = System.currentTimeMillis
    // println(s"Done. ${(end - start) / trials} ms per get.")

    // println(s"Update on large dataset ($trials trials)...")
    // start = System.currentTimeMillis
    // for (i <- 1 to trials) {
    //   val elem = r.nextInt(numElements)
    //   large = large.put(elem, 0).cache()
    // }
    // large.foreach(x => {})
    // end = System.currentTimeMillis
    // println(s"Done. ${(end - start) / trials} ms per update.")
    // val largeDerived = large.cache()

    // println(s"Insert on large dataset ($trials trials)...")
    // start = System.currentTimeMillis
    // for (i <- 1 to trials) {
    //   val elem = numElements + r.nextInt(numElements)
    //   large = large.put(elem, elem).cache()
    // }
    // large.foreach(x => {})
    // end = System.currentTimeMillis
    // println(s"Done. ${(end - start) / trials} ms per insert.")

    // println(s"Join derived dataset with original ($trials trials)...")
    // start = System.currentTimeMillis
    // for (i <- 1 to trials) {
    //   val joined = largeOrig.join(largeDerived) { (id, a, b) => a + b }.cache()
    //   joined.foreach(x => {})
    //   joined.unpersist(blocking = true)
    // }
    // end = System.currentTimeMillis
    // println(s"Done. ${(end - start) / trials} ms per join.")

    sc.stop()
  }
}
