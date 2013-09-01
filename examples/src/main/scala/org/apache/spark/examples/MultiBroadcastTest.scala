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

import org.apache.spark.SparkContext

object MultiBroadcastTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: BroadcastTest <master> [<slices>] [numElem]")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "Broadcast Test",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    val slices = if (args.length > 1) args(1).toInt else 2
    val num = if (args.length > 2) args(2).toInt else 1000000

    var arr1 = new Array[Int](num)
    for (i <- 0 until arr1.length) {
      arr1(i) = i
    }

    var arr2 = new Array[Int](num)
    for (i <- 0 until arr2.length) {
      arr2(i) = i
    }

    val barr1 = sc.broadcast(arr1)
    val barr2 = sc.broadcast(arr2)
    sc.parallelize(1 to 10, slices).foreach {
      i => println(barr1.value.size + barr2.value.size)
    }

    System.exit(0)
  }
}
