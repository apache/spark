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

package org.apache.spark.ml.stat

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}

// scalastyle:off println

class SummarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._
  import Summarizer._

  private val seed = 42
  private val eps: Double = 1e-5

  private case class ExpectedMetrics(mean: Vector, min: Vector)

  // The input is expected to be either a sparse vector, a dense vector or an array of doubles
  // (which will be converted to a dense vector)
  // The expected is the list of all the known metrics.
  //
  // The tests take an list of input vectors and a list of all the summary values that
  // are expected for this input. They currently test against some fixed subset of the
  // metrics, but should be made fuzzy in the future.

  private def testExample(input: Seq[Any], exp: ExpectedMetrics): Unit = {
    val inputVec: Seq[Vector] = input.map {
      case x: Array[Double] => Vectors.dense(x)
      case x: Vector => x
      case x => throw new Exception(x.toString)
    }
    val df = sc.parallelize(inputVec).map(Tuple1.apply).toDF("features")
    val c = df.col("features")

    compare(df.select(metrics("mean").summary(c), mean(c)), Seq(exp.mean, exp.mean))
  }

  private def denseData(input: Seq[Seq[Double]]): DataFrame = {
    val data = input.map(_.toArray).map(Vectors.dense).map(Tuple1.apply)
    sc.parallelize(data).toDF("features")
  }

  private def compare(df: DataFrame, exp: Seq[Any]): Unit = {
    println(s"compare: df=$df")
    val coll = df.collect().toSeq
    println(s"compare: coll=$coll")
    val Seq(row) = coll
    val res = row.toSeq
    println(s"compare: ress=${res.map(_.getClass)}")
    val names = df.schema.fieldNames.zipWithIndex.map { case (n, idx) => s"$n ($idx)" }
    assert(res.size === exp.size, (res.size, exp.size))
    for (((x1, x2), name) <- res.zip(exp).zip(names)) {
      compareStructures(x1, x2, name)
    }
  }

  // Compares structured content.
  private def compareStructures(x1: Any, x2: Any, name: String): Unit = (x1, x2) match {
    case (d1: Double, d2: Double) =>
      assert(Vectors.dense(d1) ~== Vectors.dense(d2) absTol 1e-4, name)
    case (r1: Row, r2: Row) =>
      assert(r1.size === r2.size, (r1, r2))
      for ((x1, x2) <- r1.toSeq.zip(r2.toSeq)) { compareStructures(x1, x2, name) }
    case (v1: Vector, v2: Vector) =>
      assert(v1 ~== v2 absTol 1e-4, name)
    case (l1: Long, l2: Long) => assert(l1 === l2)
  }

  test("debugging test") {
    val df = denseData(Nil)
    println(s">>> df=${df.collect().toSeq}")
    val c = df.col("features")
    println(s">>>>> c=$c")
    val c1 = metrics("mean").summary(c)
    println(s">>>>> c1=$c1")
    val res = df.select(c1)
    println(s">>>>> res=$res")
    intercept[SparkException] {
      compare(res, Seq.empty)
    }
  }

  test("basic error handling") {
    val df = denseData(Nil)
    val c = df.col("features")
    val res = df.select(metrics("mean").summary(c), mean(c))
    intercept[SparkException] {
      compare(res, Seq.empty)
    }
  }

  test("no element, working metrics") {
    val df = denseData(Nil)
    val c = df.col("features")
    val res = df.select(metrics("count").summary(c))
    compare(res, Seq(Row(0L)))
  }

  test("single element in vector, all metrics") {
    testExample(Seq(Array(1.0, 2.0)), ExpectedMetrics(
      Vectors.dense(1.0, 2.0),
      Vectors.dense(1.0, 2.0)
    ))
  }

  test("repeated metrics") {

  }

  test("dense vector input") {

  }

  test("sparse vector input") {

  }

  test("merging summarizer when one side has zero mean (SPARK-4355)") {
  }
}
