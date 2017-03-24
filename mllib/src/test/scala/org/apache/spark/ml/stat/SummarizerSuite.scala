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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

// scalastyle:off println

class SummarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._
  import Summarizer._

  private val seed = 42
  private val eps: Double = 1e-5

  private case class ExpectedMetrics(
      mean: Seq[Double],
      variance: Seq[Double],
      count: Long,
      numNonZeros: Seq[Long],
      max: Seq[Double],
      min: Seq[Double],
      normL2: Seq[Double],
      normL1: Seq[Double])

  // The input is expected to be either a sparse vector, a dense vector or an array of doubles
  // (which will be converted to a dense vector)
  // The expected is the list of all the known metrics.
  //
  // The tests take an list of input vectors and a list of all the summary values that
  // are expected for this input. They currently test against some fixed subset of the
  // metrics, but should be made fuzzy in the future.

  private def testExample(name: String, input: Seq[Any], exp: ExpectedMetrics): Unit = {
    def inputVec: Seq[Vector] = input.map {
      case x: Array[Double] => Vectors.dense(x)
      case x: Seq[Double] => Vectors.dense(x.toArray)
      case x: Vector => x
      case x => throw new Exception(x.toString)
    }

    def wrapped() = {
      val df = sc.parallelize(inputVec).map(Tuple1.apply).toDF("features")
      val c = df.col("features")
      df -> c
    }

    registerTest(s"$name - mean only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("mean").summary(c), mean(c)), Seq(Row(exp.mean), exp.mean))
    }

    registerTest(s"$name - mean only (direct)") {
      val (df, c) = wrapped()
      compare(df.select(mean(c)), Seq(exp.mean))
    }

    registerTest(s"$name - variance only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("variance").summary(c), variance(c)),
        Seq(Row(exp.variance), exp.variance))
    }

    registerTest(s"$name - count only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("count").summary(c), count(c)),
        Seq(Row(exp.count), exp.count))
    }

    registerTest(s"$name - numNonZeros only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("numNonZeros").summary(c), numNonZeros(c)),
        Seq(Row(exp.numNonZeros), exp.numNonZeros))
    }

    registerTest(s"$name - numNonZeros only (direct)") {
      val (df, c) = wrapped()
      compare(df.select(numNonZeros(c)),
        Seq(exp.numNonZeros))
    }

    registerTest(s"$name - min only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("min").summary(c), min(c)),
        Seq(Row(exp.min), exp.min))
    }

    registerTest(s"$name - max only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("max").summary(c), max(c)),
        Seq(Row(exp.max), exp.max))
    }

    registerTest(s"$name - normL1 only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("normL1").summary(c), normL1(c)),
        Seq(Row(exp.normL1), exp.normL1))
    }

    registerTest(s"$name - normL2 only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("normL2").summary(c), normL2(c)),
        Seq(Row(exp.normL2), exp.normL2))
    }

    registerTest(s"$name - all metrics at once") {
      val (df, c) = wrapped()
      compare(df.select(
        metrics("mean", "variance", "count", "numNonZeros").summary(c),
        mean(c), variance(c), count(c), numNonZeros(c)),
        Seq(Row(exp.mean, exp.variance, exp.count, exp.numNonZeros),
          exp.mean, exp.variance, exp.count, exp.numNonZeros))
    }
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
    println(s"compare: row=${row}")
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
    case (r1: GenericRowWithSchema, r2: Row) =>
      assert(r1.size === r2.size, (r1, r2))
      for (((fname, x1), x2) <- r1.schema.fieldNames.zip(r1.toSeq).zip(r2.toSeq)) {
        compareStructures(x1, x2, s"$name.$fname")
      }
    case (r1: Row, r2: Row) =>
      assert(r1.size === r2.size, (r1, r2))
      for ((x1, x2) <- r1.toSeq.zip(r2.toSeq)) { compareStructures(x1, x2, name) }
    case (v1: Vector, v2: Vector) =>
      assert(v1 ~== v2 absTol 1e-4, name)
    case (l1: Long, l2: Long) => assert(l1 === l2)
    case (s1: Seq[_], s2: Seq[_]) =>
      assert(s1.size === s2.size, s"$name ${(s1, s2)}")
      for (((x1, idx), x2) <- s1.zipWithIndex.zip(s2)) {
        compareStructures(x1, x2, s"$name.$idx")
      }
    case (arr1: Array[_], arr2: Array[_]) =>
      assert(arr1.toSeq === arr2.toSeq)
    case _ => throw new Exception(s"$name: ${x1.getClass} ${x2.getClass} $x1 $x2")
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
    val res = df.select(metrics("count").summary(c), count(c))
    compare(res, Seq(Row(0L), 0L))
  }

  {
    val x = Seq(0.0, 1.0, 2.0)
    testExample("single element", Seq(x), ExpectedMetrics(
      mean = x,
      variance = Seq(0.0, 0.0, 0.0),
      count = 1,
      numNonZeros = Seq(0, 1, 1),
      max = x,
      min = x,
      normL1 = x,
      normL2 = x
    ))
  }

  testExample("two elements", Seq(Seq(0.0, 1.0, 2.0), Seq(0.0, -1.0, -2.0)), ExpectedMetrics(
    mean = Seq(0.0, 0.0, 0.0),
    variance = Seq(0.0, 1.0, 2.0),
    count = 2,
    numNonZeros = Seq(0, 2, 2),
    max = Seq(0.0, 1.0, 2.0),
    min = Seq(0.0, -1.0, -2.0),
    normL1 = Seq(0.0, 2.0, 4.0),
    normL2 = Seq(0.0, 2.0, 4.0)
  ))



}
