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

package org.apache.spark.sql

import scala.language.postfixOps

import org.apache.spark.sql.test.SharedSQLContext

case class IntClass(value: Int)

class DatasetPrimitiveSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("toDS") {
    val data = Seq(1, 2, 3, 4, 5, 6)
    checkAnswer(
      data.toDS(),
      data: _*)
  }

  test("as case class / collect") {
    val ds = Seq(1, 2, 3).toDS().as[IntClass]
    checkAnswer(
      ds,
      IntClass(1), IntClass(2), IntClass(3))

    assert(ds.collect().head == IntClass(1))
  }

  test("map") {
    val ds = Seq(1, 2, 3).toDS()
    checkAnswer(
      ds.map(_ + 1),
      2, 3, 4)
  }

  test("filter") {
    val ds = Seq(1, 2, 3, 4).toDS()
    checkAnswer(
      ds.filter(_ % 2 == 0),
      2, 4)
  }

  test("foreach") {
    val ds = Seq(1, 2, 3).toDS()
    val acc = sparkContext.accumulator(0)
    ds.foreach(acc += _)
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(1, 2, 3).toDS()
    val acc = sparkContext.accumulator(0)
    ds.foreachPartition(_.foreach(acc +=))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(1, 2, 3).toDS()
    assert(ds.reduce(_ + _) == 6)
  }

  test("groupBy function, keys") {
    val ds = Seq(1, 2, 3, 4, 5).toDS()
    val grouped = ds.groupBy(_ % 2)
    checkAnswer(
      grouped.keys,
      0, 1)
  }

  test("groupBy function, map") {
    val ds = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11).toDS()
    val grouped = ds.groupBy(_ % 2)
    val agged = grouped.mapGroups { case (g, iter) =>
      val name = if (g == 0) "even" else "odd"
      (name, iter.size)
    }

    checkAnswer(
      agged,
      ("even", 5), ("odd", 6))
  }

  test("groupBy function, flatMap") {
    val ds = Seq("a", "b", "c", "xyz", "hello").toDS()
    val grouped = ds.groupBy(_.length)
    val agged = grouped.flatMapGroups { case (g, iter) => Iterator(g.toString, iter.mkString) }

    checkAnswer(
      agged,
      "1", "abc", "3", "xyz", "5", "hello")
  }

  test("Arrays and Lists") {
    checkAnswer(Seq(Seq(1)).toDS(), Seq(1))
    checkAnswer(Seq(Seq(1.toLong)).toDS(), Seq(1.toLong))
    checkAnswer(Seq(Seq(1.toDouble)).toDS(), Seq(1.toDouble))
    checkAnswer(Seq(Seq(1.toFloat)).toDS(), Seq(1.toFloat))
    checkAnswer(Seq(Seq(1.toByte)).toDS(), Seq(1.toByte))
    checkAnswer(Seq(Seq(1.toShort)).toDS(), Seq(1.toShort))
    checkAnswer(Seq(Seq(true)).toDS(), Seq(true))
    checkAnswer(Seq(Seq("test")).toDS(), Seq("test"))
    checkAnswer(Seq(Seq(Tuple1(1))).toDS(), Seq(Tuple1(1)))

    checkAnswer(Seq(Array(1)).toDS(), Array(1))
    checkAnswer(Seq(Array(1.toLong)).toDS(), Array(1.toLong))
    checkAnswer(Seq(Array(1.toDouble)).toDS(), Array(1.toDouble))
    checkAnswer(Seq(Array(1.toFloat)).toDS(), Array(1.toFloat))
    checkAnswer(Seq(Array(1.toByte)).toDS(), Array(1.toByte))
    checkAnswer(Seq(Array(1.toShort)).toDS(), Array(1.toShort))
    checkAnswer(Seq(Array(true)).toDS(), Array(true))
    checkAnswer(Seq(Array("test")).toDS(), Array("test"))
    checkAnswer(Seq(Array(Tuple1(1))).toDS(), Array(Tuple1(1)))
  }
}
