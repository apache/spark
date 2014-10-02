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
package org.apache.spark.deploy

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.util.Utils
import scala.collection.mutable

class MergablePropertiesTest extends FunSuite with Matchers {
  val test1 = "1"->"one"
  val test2 = "2"->"two"
  val test3a = "3"->"three a"
  val test3b = "3"->"three b"
  val test3c = "3"->"three c"
  val test4 = "4"->"four"

  test("merge one Map by itself") {
    val result = Utils.mergePropertyMaps(Seq(Map(test1, test2)))
    result should contain (test1)
    result should contain (test2)
  }

  test("merge two Maps no overlap") {
    val result = Utils.mergePropertyMaps(Seq(Map(test1), Map(test2)))
    result should contain (test1)
    result should contain (test2)
  }

  test("merge two maps with one level of overlap") {
    val result = Utils.mergePropertyMaps(Seq(Map(test1, test3a), Map(test2, test3b)))
    result should contain (test1)
    result should contain (test2)
    result should contain (test3a)
  }

  test("merge three maps with one level of overlap") {
    val result = Utils.mergePropertyMaps(Seq(Map(test1, test3a), Map(test2, test3b), Map(test4)))
    result should contain (test1)
    result should contain (test2)
    result should contain (test3a)
    result should contain (test4)
  }

  test("merge three maps with two levels of overlap") {
    val result = Utils.mergePropertyMaps(Seq(Map(test1, test3a), Map(test2, test3b), Map(test4, test3c)))
    result should contain (test1)
    result should contain (test2)
    result should contain (test3a)
    result should contain (test4)
  }

}
