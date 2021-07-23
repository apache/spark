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


package org.apache.spark.sql.execution.adaptive

import scala.reflect.ClassTag

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

// scalastyle:off println
class GeneralSkewSuite extends QueryTest
  with SharedSparkSession {

  private def union_splits(splits: Array[DataFrame]): DataFrame = {
    splits.reduceLeft(_ union _)
  }

  private def iterate[T: ClassTag](arrays: Array[Array[T]]): Iterator[Array[T]] = {
    require(arrays.nonEmpty)
    val iter = arrays.head.iterator.map(item => Array(item))
    arrays.tail.foldLeft(iter)(
      (iter, array) => iter.flatMap(comb => array.map(item => comb :+ item)))
  }

  private def countWithSplitted(
      splits: Array[Array[DataFrame]],
      splitted: Array[Boolean],
      countFunc: Array[DataFrame] => Long): Long = {
    val dfs = splits.zip(splitted).map {
      case (s, true) => s
      case (s, false) => Array(union_splits(s))
    }
    iterate[DataFrame](dfs).map(countFunc).sum
  }


  /**
   *                     inner
   *           /                         \
   *        inner                       right
   *      /      \                 /           \
   *    left     df3             cross        leftsemi
   *  /      \                 /     \       /       \
   * df0   inner             df4     df5  inner      full
   *       /  \                           /   \     /    \
   *     df1  df2                       df6  df7  df8  df9
   */
  test("Query 1: 10-Table Join") {
    val rng = new scala.util.Random(123)
    val splits = Array.tabulate(10) { i =>
      Array.fill(3 + rng.nextInt(3))(
        spark
          .range(rng.nextInt(10), 10 + rng.nextInt(10), 1 + rng.nextInt(2))
          .select(col("id").as("k" + i), lit(0).as("v" + i))
      )
    }
    val dfs = splits.map(union_splits)

    val countFunc = (dfs: Array[DataFrame]) => {
      require(dfs.length == 10)
      val df12 = dfs(1).join(dfs(2), col("k1") === col("k2"), "inner")
      val df012 = dfs(0).join(df12, col("k0") === col("k2"), "left")
      val df0123 = df012.join(dfs(3), col("k2") === col("k3"), "inner")
      val df45 = dfs(4).join(dfs(5), col("k4") === col("k5"), "cross")
      val df67 = dfs(6).join(dfs(7), col("k6") === col("k7"), "inner")
      val df89 = dfs(8).join(dfs(9), col("k8") === col("k9"), "full")
      val df6789 = df67.join(df89, col("k6") === col("k8"), "leftsemi")
      val df456789 = df45.join(df6789, col("k4") === col("k6"), "right")
      val dfall = df0123.join(df456789, col("k3") === col("k4"), "inner")
      dfall.count
    }

    val answer = countFunc(dfs)
    println(s"Query 1: answer = $answer")

    val splittable = Seq(0, 3, 6, 7)

    val splittableComb = Array.fill(10)(Array(false))
    splittable.foreach(i => splittableComb(i) = Array(false, true))
    iterate[Boolean](splittableComb).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 1: splitting $splitStr: $count")
      assert(answer === count)
    }

    println("Query 1: random splits")
    iterate[Boolean](Array.fill(10)(Array(false, true))).take(20).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 1: splitting $splitStr: $count")
    }
  }



  /**
   *                     inner
   *           /                         \
   *        left                       right
   *      /      \                 /           \
   *    right     df3             full        leftanti
   *  /      \                 /     \       /       \
   * df0   cross             df4     df5   left      inner
   *       /  \                           /   \     /    \
   *     df1  df2                       df6  df7  df8  df9
   */
  test("Query 2: 10-Table Join") {
    val rng = new scala.util.Random(123)
    val splits = Array.tabulate(10) { i =>
      Array.fill(3 + rng.nextInt(3))(
        spark
          .range(rng.nextInt(10), 10 + rng.nextInt(10), 1 + rng.nextInt(2))
          .select(col("id").as("k" + i), lit(0).as("v" + i))
      )
    }
    val dfs = splits.map(union_splits)

    val countFunc = (dfs: Array[DataFrame]) => {
      require(dfs.length == 10)
      val df12 = dfs(1).join(dfs(2), col("k1") === col("k2"), "cross")
      val df012 = dfs(0).join(df12, col("k0") === col("k2"), "right")
      val df0123 = df012.join(dfs(3), col("k2") === col("k3"), "left")
      val df45 = dfs(4).join(dfs(5), col("k4") === col("k5"), "full")
      val df67 = dfs(6).join(dfs(7), col("k6") === col("k7"), "left")
      val df89 = dfs(8).join(dfs(9), col("k8") === col("k9"), "inner")
      val df6789 = df67.join(df89, col("k6") === col("k8"), "leftanti")
      val df456789 = df45.join(df6789, col("k4") === col("k6"), "right")
      val dfall = df0123.join(df456789, col("k3") === col("k4"), "inner")
      dfall.count
    }

    val answer = countFunc(dfs)
    println(s"Query 2: answer = $answer")

    val splittable = Seq(1, 2, 6)

    val splittableComb = Array.fill(10)(Array(false))
    splittable.foreach(i => splittableComb(i) = Array(false, true))
    iterate[Boolean](splittableComb).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 2: splitting $splitStr: $count")
      assert(answer === count)
    }

    println("Query 2: random splits")
    iterate[Boolean](Array.fill(10)(Array(false, true))).take(20).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 2: splitting $splitStr: $count")
    }
  }



  /**
   *                     inner
   *           /                         \
   *        left                       cross
   *      /      \                 /           \
   *    right     df3            inner         full
   *  /      \                 /     \       /       \
   * df0   right             df4     df5   left    leftanti
   *       /  \                           /   \     /    \
   *     df1  df2                       df6  df7  df8  df9
   */
  test("Query 3: 10-Table Join") {
    val rng = new scala.util.Random(123)
    val splits = Array.tabulate(10) { i =>
      Array.fill(3 + rng.nextInt(3))(
        spark
          .range(rng.nextInt(10), 10 + rng.nextInt(10), 1 + rng.nextInt(2))
          .select(col("id").as("k" + i), lit(0).as("v" + i))
      )
    }
    val dfs = splits.map(union_splits)

    val countFunc = (dfs: Array[DataFrame]) => {
      require(dfs.length == 10)
      val df12 = dfs(1).join(dfs(2), col("k1") === col("k2"), "right")
      val df012 = dfs(0).join(df12, col("k0") === col("k2"), "right")
      val df0123 = df012.join(dfs(3), col("k2") === col("k3"), "left")
      val df45 = dfs(4).join(dfs(5), col("k4") === col("k5"), "inner")
      val df67 = dfs(6).join(dfs(7), col("k6") === col("k7"), "left")
      val df89 = dfs(8).join(dfs(9), col("k8") === col("k9"), "leftanti")
      val df6789 = df67.join(df89, col("k6") === col("k8"), "full")
      val df456789 = df45.join(df6789, col("k4") === col("k6"), "cross")
      val dfall = df0123.join(df456789, col("k3") === col("k4"), "inner")
      dfall.count
    }

    val answer = countFunc(dfs)
    println(s"Query 3: answer = $answer")

    val splittable = Seq(2, 4, 5)

    val splittableComb = Array.fill(10)(Array(false))
    splittable.foreach(i => splittableComb(i) = Array(false, true))
    iterate[Boolean](splittableComb).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 3: splitting $splitStr: $count")
      assert(answer === count)
    }

    println("Query 3: random splits")
    iterate[Boolean](Array.fill(10)(Array(false, true))).take(20).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 3: splitting $splitStr: $count")
    }
  }



  /**
   *                     right
   *           /                         \
   *        inner                       cross
   *      /      \                 /           \
   *    left     df3            inner         right
   *  /      \                 /     \       /       \
   * df0   right             df4     df5   left    leftanti
   *       /  \                           /   \     /    \
   *     df1  df2                       df6  df7  df8  df9
   */
  test("Query 4: 10-Table Join") {
    val rng = new scala.util.Random(123)
    val splits = Array.tabulate(10) { i =>
      Array.fill(3 + rng.nextInt(3))(
        spark
          .range(rng.nextInt(10), 10 + rng.nextInt(10), 1 + rng.nextInt(2))
          .select(col("id").as("k" + i), lit(0).as("v" + i))
      )
    }
    val dfs = splits.map(union_splits)

    val countFunc = (dfs: Array[DataFrame]) => {
      require(dfs.length == 10)
      val df12 = dfs(1).join(dfs(2), col("k1") === col("k2"), "right")
      val df012 = dfs(0).join(df12, col("k0") === col("k2"), "left")
      val df0123 = df012.join(dfs(3), col("k2") === col("k3"), "inner")
      val df45 = dfs(4).join(dfs(5), col("k4") === col("k5"), "inner")
      val df67 = dfs(6).join(dfs(7), col("k6") === col("k7"), "left")
      val df89 = dfs(8).join(dfs(9), col("k8") === col("k9"), "leftanti")
      val df6789 = df67.join(df89, col("k6") === col("k8"), "right")
      val df456789 = df45.join(df6789, col("k4") === col("k6"), "cross")
      val dfall = df0123.join(df456789, col("k3") === col("k4"), "right")
      dfall.count
    }

    val answer = countFunc(dfs)
    println(s"Query 4: answer = $answer")

    val splittable = Seq(4, 5, 8)

    val splittableComb = Array.fill(10)(Array(false))
    splittable.foreach(i => splittableComb(i) = Array(false, true))
    iterate[Boolean](splittableComb).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 4: splitting $splitStr: $count")
      assert(answer === count)
    }

    println("Query 4: random splits")
    iterate[Boolean](Array.fill(10)(Array(false, true))).take(20).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 4: splitting $splitStr: $count")
    }
  }



  /**
   *                     inner
   *             /                      \
   *            /                      agg
   *           /                         \
   *        left                       cross
   *      /      \                 /           \
   *    right     df3            inner         full
   *  /      \                 /     \       /       \
   * df0   inner             df4     df5   left    leftanti
   *       /  \                           /   \     /    \
   *     df1  df2                       df6  df7  df8  df9
   */
  test("Query 5: 10-Table Join with Agg") {
    val rng = new scala.util.Random(123)
    val splits = Array.tabulate(10) { i =>
      Array.fill(3 + rng.nextInt(3))(
        spark
          .range(rng.nextInt(10), 10 + rng.nextInt(10), 1 + rng.nextInt(2))
          .select(col("id").as("k" + i), lit(0).as("v" + i))
      )
    }
    val dfs = splits.map(union_splits)

    val countFunc = (dfs: Array[DataFrame]) => {
      require(dfs.length == 10)
      val df12 = dfs(1).join(dfs(2), col("k1") === col("k2"), "inner")
      val df012 = dfs(0).join(df12, col("k0") === col("k2"), "right")
      val df0123 = df012.join(dfs(3), col("k2") === col("k3"), "left")
      val df45 = dfs(4).join(dfs(5), col("k4") === col("k5"), "inner")
      val df67 = dfs(6).join(dfs(7), col("k6") === col("k7"), "left")
      val df89 = dfs(8).join(dfs(9), col("k8") === col("k9"), "leftanti")
      val df6789 = df67.join(df89, col("k6") === col("k8"), "full")
      val df456789 = df45.join(df6789, col("k4") === col("k6"), "cross")
        .groupBy("k4").agg(max("v6"))
      val dfall = df0123.join(df456789, col("k3") === col("k4"), "inner")
      dfall.count
    }

    val answer = countFunc(dfs)
    println(s"Query 5: answer = $answer")

    val splittable = Seq(1, 2)

    val splittableComb = Array.fill(10)(Array(false))
    splittable.foreach(i => splittableComb(i) = Array(false, true))
    iterate[Boolean](splittableComb).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 5: splitting $splitStr: $count")
      assert(answer === count)
    }

    println("Query 5: random splits")
    iterate[Boolean](Array.fill(10)(Array(false, true))).take(20).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 5: splitting $splitStr: $count")
    }
  }



  /**
   *                     cross
   *             /                     \
   *           agg                      \
   *           /                         \
   *        left                       cross
   *      /      \                 /           \
   *    inner     df3            agg          inner
   *  /      \                   /          /       \
   * df0   right              inner      inner     left
   *       /  \               /   \      /   \     /    \
   *     df1  df2           df4  df5    df6  df7  df8  df9
   */
  test("Query 6: 10-Table Join with Agg") {
    val rng = new scala.util.Random(123)
    val splits = Array.tabulate(10) { i =>
      Array.fill(3 + rng.nextInt(3))(
        spark
          .range(rng.nextInt(10), 10 + rng.nextInt(10), 1 + rng.nextInt(2))
          .select(col("id").as("k" + i), lit(0).as("v" + i))
      )
    }
    val dfs = splits.map(union_splits)

    val countFunc = (dfs: Array[DataFrame]) => {
      require(dfs.length == 10)
      val df12 = dfs(1).join(dfs(2), col("k1") === col("k2"), "right")
      val df012 = dfs(0).join(df12, col("k0") === col("k2"), "inner")
      val df0123 = df012.join(dfs(3), col("k2") === col("k3"), "left")
        .groupBy("k3").agg(max("v1"))
      val df45 = dfs(4).join(dfs(5), col("k4") === col("k5"), "inner")
        .groupBy("k4").agg(max("v5"))
      val df67 = dfs(6).join(dfs(7), col("k6") === col("k7"), "inner")
      val df89 = dfs(8).join(dfs(9), col("k8") === col("k9"), "left")
      val df6789 = df67.join(df89, col("k6") === col("k8"), "inner")
      val df456789 = df45.join(df6789, col("k4") === col("k6"), "cross")
      val dfall = df0123.join(df456789, col("k3") === col("k4"), "cross")
      dfall.count
    }

    val answer = countFunc(dfs)
    println(s"Query 6: answer = $answer")

    val splittable = Seq(6, 7, 8)

    val splittableComb = Array.fill(10)(Array(false))
    splittable.foreach(i => splittableComb(i) = Array(false, true))
    iterate[Boolean](splittableComb).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 6: splitting $splitStr: $count")
      assert(answer === count)
    }

    println("Query 6: random splits")
    iterate[Boolean](Array.fill(10)(Array(false, true))).take(20).foreach { splitted =>
      val count = countWithSplitted(splits, splitted, countFunc)
      val splitStr = splitted.zipWithIndex.filter(_._1).map(_._2).mkString("[", ",", "]")
      println(s"Query 6: splitting $splitStr: $count")
    }
  }
}
// scalastyle:on println
