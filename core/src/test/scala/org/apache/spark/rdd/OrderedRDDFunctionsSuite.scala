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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.util.Random

import org.scalatest.FunSuite
import com.google.common.io.Files
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.conf.{Configuration, Configurable}

import org.apache.spark.SparkContext._
import org.apache.spark.{Partitioner, SharedSparkContext}

class OrderedRDDFunctionsSuite extends FunSuite with SharedSparkContext {
  test("mergeJoin") {
    val rdd1 = sc.parallelize(Array((3, 1), (1, 2), (2, 1), (1, 1)))
    val rdd2 = sc.parallelize(Array((2, 'z'), (1, 'x'), (4, 'w'), (2, 'y')))
    val joined = rdd1.mergeJoin(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSeq === Seq(
      (1, (2, 'x')),
      (1, (1, 'x')),
      (2, (1, 'z')),
      (2, (1, 'y'))
    ))
  }

  test("mergeJoin pre-ordered") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.mergeJoin(rdd2, true).collect()
    assert(joined.size === 4)
    assert(joined.toSeq === Seq(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("mergeJoin all-to-all") {
    val rdd1 = sc.parallelize(Array((1, 2), (1, 1), (1, 3)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')))
    val joined = rdd1.mergeJoin(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSeq === Seq(
      (1, (2, 'x')),
      (1, (2, 'y')),
      (1, (1, 'x')),
      (1, (1, 'y')),
      (1, (3, 'x')),
      (1, (3, 'y'))
    ))
  }

  test("mergeJoin all-to-all pre-ordered") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')))
    val joined = rdd1.mergeJoin(rdd2, true).collect()
    assert(joined.size === 6)
    assert(joined.toSeq === Seq(
      (1, (1, 'x')),
      (1, (1, 'y')),
      (1, (2, 'x')),
      (1, (2, 'y')),
      (1, (3, 'x')),
      (1, (3, 'y'))
    ))
  }

  test("mergeLeftOuterJoin") {
    val rdd1 = sc.parallelize(Array((3, 1), (1, 2), (2, 1), (1, 1)))
    val rdd2 = sc.parallelize(Array((2, 'z'), (1, 'x'), (4, 'w'), (2, 'y')))
    val joined = rdd1.mergeLeftOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSeq === Seq(
      (1, (2, Some('x'))),
      (1, (1, Some('x'))),
      (2, (1, Some('z'))),
      (2, (1, Some('y'))),
      (3, (1, None))
    ))
  }

  test("mergeLeftOuterJoin pre-ordered") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.mergeLeftOuterJoin(rdd2, true).collect()
    assert(joined.size === 5)
    assert(joined.toSeq === Seq(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))
  }

  test("mergeRightOuterJoin") {
    val rdd1 = sc.parallelize(Array((3, 1), (1, 2), (2, 1), (1, 1)))
    val rdd2 = sc.parallelize(Array((2, 'z'), (1, 'x'), (4, 'w'), (2, 'y')))
    val joined = rdd1.mergeRightOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSeq === Seq(
      (1, (Some(2), 'x')),
      (1, (Some(1), 'x')),
      (2, (Some(1), 'z')),
      (2, (Some(1), 'y')),
      (4, (None, 'w'))
    ))
  }

  test("mergeRightOuterJoin pre-ordered") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.mergeRightOuterJoin(rdd2, true).collect()
    assert(joined.size === 5)
    assert(joined.toSeq === Seq(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))
  }

  test("mergeJoin with no matches") {
    val rdd1 = sc.parallelize(Array((3, 1), (1, 2), (2, 1), (1, 1)))
    val rdd2 = sc.parallelize(Array((5, 'z'), (4, 'x'), (6, 'w'), (5, 'y')))
    val joined = rdd1.mergeJoin(rdd2).collect()
    assert(joined.size === 0)
  }

  test("mergeJoin with no matches pre-ordered") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')))
    val joined = rdd1.mergeJoin(rdd2, true).collect()
    assert(joined.size === 0)
  }

  test("mergeJoin with many output partitions") {
    val rdd1 = sc.parallelize(Array((3, 1), (1, 2), (2, 1), (1, 1)))
    val rdd2 = sc.parallelize(Array((2, 'z'), (1, 'x'), (4, 'w'), (2, 'y')))
    val joined = rdd1.mergeJoin(rdd2, 10).collect()
    assert(joined.size === 4)
    assert(joined.toSeq === Seq(
      (1, (2, 'x')),
      (1, (1, 'x')),
      (2, (1, 'z')),
      (2, (1, 'y'))
    ))
  }

  test("mergeJoin with many output partitions pre-ordered") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.mergeJoin(rdd2, 10, true).collect()
    assert(joined.size === 4)
    assert(joined.toSeq === Seq(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }
}
