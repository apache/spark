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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

case class ClassData(a: String, b: Int)

class DatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("toDS") {
    val data = Seq(("a", 1) , ("b", 2), ("c", 3))
    checkAnswer(
      data.toDS(),
      data: _*)
  }

  test("toDS with RDD") {
    val ds = sparkContext.makeRDD(Seq("a", "b", "c"), 3).toDS()
    checkAnswer(
      ds.mapPartitions(_ => Iterator(1)),
      1, 1, 1)
  }

  test("as tuple") {
    val data = Seq(("a", 1), ("b", 2)).toDF("a", "b")
    checkAnswer(
      data.as[(String, Int)],
      ("a", 1), ("b", 2))
  }

  test("as case class / collect") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDF("a", "b").as[ClassData]
    checkAnswer(
      ds,
      ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))
    assert(ds.collect().head == ClassData("a", 1))
  }

  test("as case class - reordered fields by name") {
    val ds = Seq((1, "a"), (2, "b"), (3, "c")).toDF("b", "a").as[ClassData]
    assert(ds.collect() === Array(ClassData("a", 1), ClassData("b", 2), ClassData("c", 3)))
  }

  test("as case class - take") {
    val ds = Seq((1, "a"), (2, "b"), (3, "c")).toDF("b", "a").as[ClassData]
    assert(ds.take(2) === Array(ClassData("a", 1), ClassData("b", 2)))
  }

  test("map") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.map(v => (v._1, v._2 + 1)),
      ("a", 2), ("b", 3), ("c", 4))
  }

  test("select") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(expr("_2 + 1").as[Int]),
      2, 3, 4)
  }

  test("select 2") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(
        expr("_1").as[String],
        expr("_2").as[Int]) : Dataset[(String, Int)],
      ("a", 1), ("b", 2), ("c", 3))
  }

  test("select 2, primitive and tuple") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(
        expr("_1").as[String],
        expr("struct(_2, _2)").as[(Int, Int)]),
      ("a", (1, 1)), ("b", (2, 2)), ("c", (3, 3)))
  }

  test("select 2, primitive and class") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
  }

  test("select 2, primitive and class, fields reordered") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkDecoding(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('b', _2, 'a', _1)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
  }

  test("filter") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.filter(_._1 == "b"),
      ("b", 2))
  }

  test("foreach") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.accumulator(0)
    ds.foreach(v => acc += v._2)
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.accumulator(0)
    ds.foreachPartition(_.foreach(v => acc += v._2))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    assert(ds.reduce((a, b) => ("sum", a._2 + b._2)) == ("sum", 6))
  }

  test("joinWith, flat schema") {
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")

    checkAnswer(
      ds1.joinWith(ds2, $"a.value" === $"b.value"),
      (1, 1), (2, 2))
  }

  test("joinWith, expression condition") {
    val ds1 = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()
    val ds2 = Seq(("a", 1), ("b", 2)).toDS()

    checkAnswer(
      ds1.joinWith(ds2, $"_1" === $"a"),
      (ClassData("a", 1), ("a", 1)), (ClassData("b", 2), ("b", 2)))
  }

  test("joinWith tuple with primitive, expression") {
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(("a", 1), ("b", 2)).toDS()

    checkAnswer(
      ds1.joinWith(ds2, $"value" === $"_2"),
      (1, ("a", 1)), (1, ("a", 1)), (2, ("b", 2)))
  }

  test("joinWith class with primitive, toDF") {
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()

    checkAnswer(
      ds1.joinWith(ds2, $"value" === $"b").toDF().select($"_1", $"_2.a", $"_2.b"),
      Row(1, "a", 1) :: Row(1, "a", 1) :: Row(2, "b", 2) :: Nil)
  }

  test("multi-level joinWith") {
    val ds1 = Seq(("a", 1), ("b", 2)).toDS().as("a")
    val ds2 = Seq(("a", 1), ("b", 2)).toDS().as("b")
    val ds3 = Seq(("a", 1), ("b", 2)).toDS().as("c")

    checkAnswer(
      ds1.joinWith(ds2, $"a._2" === $"b._2").as("ab").joinWith(ds3, $"ab._1._2" === $"c._2"),
      ((("a", 1), ("a", 1)), ("a", 1)),
      ((("b", 2), ("b", 2)), ("b", 2)))

  }

  test("groupBy function, keys") {
    val ds = Seq(("a", 1), ("b", 1)).toDS()
    val grouped = ds.groupBy(v => (1, v._2))
    checkAnswer(
      grouped.keys,
      (1, 1))
  }

  test("groupBy function, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy(v => (v._1, "word"))
    val agged = grouped.map { case (g, iter) => (g._1, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy function, fatMap") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy(v => (v._1, "word"))
    val agged = grouped.flatMap { case (g, iter) => Iterator(g._1, iter.map(_._2).sum.toString) }

    checkAnswer(
      agged,
      "a", "30", "b", "3", "c", "1")
  }

  test("groupBy function, reduce") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val agged = ds.groupBy(_.length).reduce(_ + _)

    checkAnswer(
      agged,
      3 -> "abcxyz", 5 -> "hello")
  }

  test("groupBy columns, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1")
    val agged = grouped.map { case (g, iter) => (g.getString(0), iter.map(_._2).sum) }

    checkAnswer(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy columns asKey, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1").asKey[String]
    val agged = grouped.map { case (g, iter) => (g, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy columns asKey tuple, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1", lit(1)).asKey[(String, Int)]
    val agged = grouped.map { case (g, iter) => (g, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      (("a", 1), 30), (("b", 1), 3), (("c", 1), 1))
  }

  test("groupBy columns asKey class, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1".as("a"), lit(1).as("b")).asKey[ClassData]
    val agged = grouped.map { case (g, iter) => (g, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      (ClassData("a", 1), 30), (ClassData("b", 1), 3), (ClassData("c", 1), 1))
  }

  test("typed aggregation: expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(sum("_2").as[Int]),
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("typed aggregation: expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(sum("_2").as[Int], sum($"_2" + 1).as[Long]),
      ("a", 30, 32L), ("b", 3, 5L), ("c", 1, 2L))
  }

  test("typed aggregation: expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(sum("_2").as[Int], sum($"_2" + 1).as[Long], count("*").as[Long]),
      ("a", 30, 32L, 2L), ("b", 3, 5L, 2L), ("c", 1, 2L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(
        sum("_2").as[Int],
        sum($"_2" + 1).as[Long],
        count("*").as[Long],
        avg("_2").as[Double]),
      ("a", 30, 32L, 2L, 15.0), ("b", 3, 5L, 2L, 1.5), ("c", 1, 2L, 1L, 1.0))
  }

  test("cogroup") {
    val ds1 = Seq(1 -> "a", 3 -> "abc", 5 -> "hello", 3 -> "foo").toDS()
    val ds2 = Seq(2 -> "q", 3 -> "w", 5 -> "e", 5 -> "r").toDS()
    val cogrouped = ds1.groupBy(_._1).cogroup(ds2.groupBy(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2).mkString + "#" + data2.map(_._2).mkString))
    }

    checkAnswer(
      cogrouped,
      1 -> "a#", 2 -> "#q", 3 -> "abcfoo#w", 5 -> "hello#er")
  }

  test("SPARK-11436: we should rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    checkAnswer(joined, ("2", 2))
  }

  test("toString") {
    val ds = Seq((1, 2)).toDS()
    assert(ds.toString == "[_1: int, _2: int]")
  }
}
