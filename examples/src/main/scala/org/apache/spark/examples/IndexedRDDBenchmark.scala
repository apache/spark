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

    val numElementsLarge = numPartitions * elemsPerPartition

    val conf = new SparkConf()
      .setAppName(s"IndexedRDD Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val r = new util.Random(0)

    println("Constructing large dataset...")
    var large = IndexedRDD(sc.parallelize(0 until numPartitions, numPartitions).flatMap(p =>
      (p * elemsPerPartition) until ((p + 1) * elemsPerPartition))
      .map(x => (x.toLong, x))).cache()
    val largeOrig = large
    println(s"Done. Generated ${large.count} elements.")

    println(s"Get on large dataset ($trials trials)...")
    var start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val elem = r.nextInt(numElementsLarge)
      val value = large.get(elem)
      assert(value == Some(elem), s"get($elem) was $value")
    }
    var end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per get.")

    println(s"Update on large dataset ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val elem = r.nextInt(numElementsLarge)
      large = large.put(elem, 0).cache()
    }
    large.foreach(x => {})
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per update.")
    val largeDerived = large.cache()

    println(s"Insert on large dataset ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val elem = numElementsLarge + r.nextInt(numElementsLarge)
      large = large.put(elem, elem).cache()
    }
    large.foreach(x => {})
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per insert.")

    println(s"Join derived dataset with original ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val joined = largeOrig.join(largeDerived) { (id, a, b) => a + b }.cache()
      joined.foreach(x => {})
      joined.unpersist(blocking = true)
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per join.")

    sc.stop()
  }
}
