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

package org.apache.spark.rdd

import org.apache.spark.{SparkContext, LocalSparkContext}
import org.scalatest.{Matchers, FunSuite}

import org.apache.spark.SparkContext._

class ZippedWithIndexRDDSuite extends FunSuite with Matchers with LocalSparkContext {

  val clusterUrl = "local-cluster[3,1,512]"

  test("zipWithIndex with distinct operation") {
    sc = new SparkContext(clusterUrl, "zipWithIndex test")
    val c = sc.parallelize(1 to 189, 3).flatMap { i =>
      (1 to 1000).toSeq.map(p => i * 600 + p)
    }.distinct().zipWithIndex()
    val count = c.join(c).filter(t => t._2._1 != t._2._2).count
    assert(count === 0)
  }
}
