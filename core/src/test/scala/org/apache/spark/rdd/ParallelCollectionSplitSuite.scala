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

import scala.collection.immutable.NumericRange

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class ParallelCollectionSplitSuite extends FunSuite with Checkers {
  test("one element per slice") {
    val data = Array(1, 2, 3)
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices(0).mkString(",") === "1")
    assert(slices(1).mkString(",") === "2")
    assert(slices(2).mkString(",") === "3")
  }

  test("one slice") {
    val data = Array(1, 2, 3)
    val slices = ParallelCollectionRDD.slice(data, 1)
    assert(slices.size === 1)
    assert(slices(0).mkString(",") === "1,2,3")
  }

  test("equal slices") {
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices(0).mkString(",") === "1,2,3")
    assert(slices(1).mkString(",") === "4,5,6")
    assert(slices(2).mkString(",") === "7,8,9")
  }

  test("non-equal slices") {
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices(0).mkString(",") === "1,2,3")
    assert(slices(1).mkString(",") === "4,5,6")
    assert(slices(2).mkString(",") === "7,8,9,10")
  }

  test("splitting exclusive range") {
    val data = 0 until 100
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices(0).mkString(",") === (0 to 32).mkString(","))
    assert(slices(1).mkString(",") === (33 to 65).mkString(","))
    assert(slices(2).mkString(",") === (66 to 99).mkString(","))
  }

  test("splitting inclusive range") {
    val data = 0 to 100
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices(0).mkString(",") === (0 to 32).mkString(","))
    assert(slices(1).mkString(",") === (33 to 66).mkString(","))
    assert(slices(2).mkString(",") === (67 to 100).mkString(","))
    assert(slices(2).isInstanceOf[Range.Inclusive])
  }

  test("empty data") {
    val data = new Array[Int](0)
    val slices = ParallelCollectionRDD.slice(data, 5)
    assert(slices.size === 5)
    for (slice <- slices) assert(slice.size === 0)
  }

  test("zero slices") {
    val data = Array(1, 2, 3)
    intercept[IllegalArgumentException] { ParallelCollectionRDD.slice(data, 0) }
  }

  test("negative number of slices") {
    val data = Array(1, 2, 3)
    intercept[IllegalArgumentException] { ParallelCollectionRDD.slice(data, -5) }
  }

  test("exclusive ranges sliced into ranges") {
    val data = 1 until 100
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices.map(_.size).reduceLeft(_+_) === 99)
    assert(slices.forall(_.isInstanceOf[Range]))
  }

  test("inclusive ranges sliced into ranges") {
    val data = 1 to 100
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices.map(_.size).reduceLeft(_+_) === 100)
    assert(slices.forall(_.isInstanceOf[Range]))
  }

  test("identical slice sizes between Range and NumericRange") {
    val r = ParallelCollectionRDD.slice(1 to 7, 4)
    val nr = ParallelCollectionRDD.slice(1L to 7L, 4)
    assert(r.size === 4)
    for (i <- 0 until r.size) {
      assert(r(i).size === nr(i).size)
    }
  }

  test("identical slice sizes between List and NumericRange") {
    val r = ParallelCollectionRDD.slice(List(1, 2), 4)
    val nr = ParallelCollectionRDD.slice(1L to 2L, 4)
    assert(r.size === 4)
    for (i <- 0 until r.size) {
      assert(r(i).size === nr(i).size)
    }
  }

  test("large ranges don't overflow") {
    val N = 100 * 1000 * 1000
    val data = 0 until N
    val slices = ParallelCollectionRDD.slice(data, 40)
    assert(slices.size === 40)
    for (i <- 0 until 40) {
      assert(slices(i).isInstanceOf[Range])
      val range = slices(i).asInstanceOf[Range]
      assert(range.start === i * (N / 40), "slice " + i + " start")
      assert(range.end   === (i+1) * (N / 40), "slice " + i + " end")
      assert(range.step  === 1, "slice " + i + " step")
    }
  }

  test("random array tests") {
    val gen = for {
      d <- arbitrary[List[Int]]
      n <- Gen.choose(1, 100)
    } yield (d, n)
    val prop = forAll(gen) {
      (tuple: (List[Int], Int)) =>
        val d = tuple._1
        val n = tuple._2
        val slices = ParallelCollectionRDD.slice(d, n)
        ("n slices"    |: slices.size == n) &&
        ("concat to d" |: Seq.concat(slices: _*).mkString(",") == d.mkString(",")) &&
        ("equal sizes" |: slices.map(_.size).forall(x => x==d.size/n || x==d.size/n+1))
    }
    check(prop)
  }

  test("random exclusive range tests") {
    val gen = for {
      a <- Gen.choose(-100, 100)
      b <- Gen.choose(-100, 100)
      step <- Gen.choose(-5, 5) suchThat (_ != 0)
      n <- Gen.choose(1, 100)
    } yield (a until b by step, n)
    val prop = forAll(gen) {
      case (d: Range, n: Int) =>
        val slices = ParallelCollectionRDD.slice(d, n)
        ("n slices"    |: slices.size == n) &&
        ("all ranges"  |: slices.forall(_.isInstanceOf[Range])) &&
        ("concat to d" |: Seq.concat(slices: _*).mkString(",") == d.mkString(",")) &&
        ("equal sizes" |: slices.map(_.size).forall(x => x==d.size/n || x==d.size/n+1))
    }
    check(prop)
  }

  test("random inclusive range tests") {
    val gen = for {
      a <- Gen.choose(-100, 100)
      b <- Gen.choose(-100, 100)
      step <- Gen.choose(-5, 5) suchThat (_ != 0)
      n <- Gen.choose(1, 100)
    } yield (a to b by step, n)
    val prop = forAll(gen) {
      case (d: Range, n: Int) =>
        val slices = ParallelCollectionRDD.slice(d, n)
        ("n slices"    |: slices.size == n) &&
        ("all ranges"  |: slices.forall(_.isInstanceOf[Range])) &&
        ("concat to d" |: Seq.concat(slices: _*).mkString(",") == d.mkString(",")) &&
        ("equal sizes" |: slices.map(_.size).forall(x => x==d.size/n || x==d.size/n+1))
    }
    check(prop)
  }

  test("exclusive ranges of longs") {
    val data = 1L until 100L
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices.map(_.size).reduceLeft(_+_) === 99)
    assert(slices.forall(_.isInstanceOf[NumericRange[_]]))
  }

  test("inclusive ranges of longs") {
    val data = 1L to 100L
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices.map(_.size).reduceLeft(_+_) === 100)
    assert(slices.forall(_.isInstanceOf[NumericRange[_]]))
  }

  test("exclusive ranges of doubles") {
    val data = 1.0 until 100.0 by 1.0
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices.map(_.size).reduceLeft(_+_) === 99)
    assert(slices.forall(_.isInstanceOf[NumericRange[_]]))
  }

  test("inclusive ranges of doubles") {
    val data = 1.0 to 100.0 by 1.0
    val slices = ParallelCollectionRDD.slice(data, 3)
    assert(slices.size === 3)
    assert(slices.map(_.size).reduceLeft(_+_) === 100)
    assert(slices.forall(_.isInstanceOf[NumericRange[_]]))
  }

  test("inclusive ranges with Int.MaxValue and Int.MinValue") {
    val data1 = 1 to Int.MaxValue
    val slices1 = ParallelCollectionRDD.slice(data1, 3)
    assert(slices1.size === 3)
    assert(slices1.map(_.size).sum === Int.MaxValue)
    assert(slices1(2).isInstanceOf[Range.Inclusive])
    val data2 = -2 to Int.MinValue by -1
    val slices2 = ParallelCollectionRDD.slice(data2, 3)
    assert(slices2.size == 3)
    assert(slices2.map(_.size).sum === Int.MaxValue)
    assert(slices2(2).isInstanceOf[Range.Inclusive])
  }

  test("empty ranges with Int.MaxValue and Int.MinValue") {
    val data1 = Int.MaxValue until Int.MaxValue
    val slices1 = ParallelCollectionRDD.slice(data1, 5)
    assert(slices1.size === 5)
    for (i <- 0 until 5) assert(slices1(i).size === 0)
    val data2 = Int.MaxValue until Int.MaxValue
    val slices2 = ParallelCollectionRDD.slice(data2, 5)
    assert(slices2.size === 5)
    for (i <- 0 until 5) assert(slices2(i).size === 0)
  }
}
