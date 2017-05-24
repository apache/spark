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

// scalastyle:off println
package org.apache.spark.examples

import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Asynchronous Actions.
  *
  * This is an example how to use Asynchronous Actions. For more conventional use,
  * please refer to org.apache.spark.rdd.AsyncRDDActions
  */

object SparkAsync {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark_async").set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(32, 34, 2, 3, 4, 54, 3), 4).cache()
    rdd.collectAsync().map { x => x.map { x => println("Items in the list:" + x) } }
    val rddCount = sc.parallelize(List(434, 3, 2, 43, 45, 3, 2), 4)
    rddCount.countAsync().map { x => println("Number of items in the list: " + x) }
    println("synchronous count" + rdd.count())
    sc.stop()
  }
}
// scalastyle:on println
