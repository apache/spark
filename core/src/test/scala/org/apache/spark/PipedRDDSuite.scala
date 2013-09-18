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

package org.apache.spark

import org.scalatest.FunSuite
import SparkContext._

class PipedRDDSuite extends FunSuite with SharedSparkContext {

  test("basic pipe") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val piped = nums.pipe(Seq("cat"))

    val c = piped.collect()
    assert(c.size === 4)
    assert(c(0) === "1")
    assert(c(1) === "2")
    assert(c(2) === "3")
    assert(c(3) === "4")
  }

  test("advanced pipe") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val bl = sc.broadcast(List("0"))

    val piped = nums.pipe(Seq("cat"),
      Map[String, String](),
      (f: String => Unit) => {bl.value.map(f(_));f("\u0001")},
      (i:Int, f: String=> Unit) => f(i + "_"))

    val c = piped.collect()

    assert(c.size === 8)
    assert(c(0) === "0")
    assert(c(1) === "\u0001")
    assert(c(2) === "1_")
    assert(c(3) === "2_")
    assert(c(4) === "0")
    assert(c(5) === "\u0001")
    assert(c(6) === "3_")
    assert(c(7) === "4_")

    val nums1 = sc.makeRDD(Array("a\t1", "b\t2", "a\t3", "b\t4"), 2)
    val d = nums1.groupBy(str=>str.split("\t")(0)).
      pipe(Seq("cat"),
           Map[String, String](),
           (f: String => Unit) => {bl.value.map(f(_));f("\u0001")},
           (i:Tuple2[String, Seq[String]], f: String=> Unit) => {for (e <- i._2){ f(e + "_")}}).collect()
    assert(d.size === 8)
    assert(d(0) === "0")
    assert(d(1) === "\u0001")
    assert(d(2) === "b\t2_")
    assert(d(3) === "b\t4_")
    assert(d(4) === "0")
    assert(d(5) === "\u0001")
    assert(d(6) === "a\t1_")
    assert(d(7) === "a\t3_")
  }

  test("pipe with env variable") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val piped = nums.pipe(Seq("printenv", "MY_TEST_ENV"), Map("MY_TEST_ENV" -> "LALALA"))
    val c = piped.collect()
    assert(c.size === 2)
    assert(c(0) === "LALALA")
    assert(c(1) === "LALALA")
  }

  test("pipe with non-zero exit status") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val piped = nums.pipe(Seq("cat nonexistent_file", "2>", "/dev/null"))
    intercept[SparkException] {
      piped.collect()
    }
  }

}
