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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.SummaryBuilderImpl.Buffer
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, Statistics}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class SummarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._
  import Summarizer._

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
      case x: Array[Double @unchecked] => Vectors.dense(x)
      case x: Seq[Double @unchecked] => Vectors.dense(x.toArray)
      case x: Vector => x
      case x => throw new Exception(x.toString)
    }

    val s = {
      val s2 = new MultivariateOnlineSummarizer
      inputVec.foreach(v => s2.add(OldVectors.fromML(v)))
      s2
    }

    // Because the Spark context is reset between tests, we cannot hold a reference onto it.
    def wrapped() = {
      val df = sc.parallelize(inputVec).map(Tuple1.apply).toDF("features")
      val c = df.col("features")
      (df, c)
    }

    registerTest(s"$name - mean only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("mean").summary(c), mean(c)), Seq(Row(exp.mean), s.mean))
    }

    registerTest(s"$name - mean only (direct)") {
      val (df, c) = wrapped()
      compare(df.select(mean(c)), Seq(exp.mean))
    }

    registerTest(s"$name - variance only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("variance").summary(c), variance(c)),
        Seq(Row(exp.variance), s.variance))
    }

    registerTest(s"$name - variance only (direct)") {
      val (df, c) = wrapped()
      compare(df.select(variance(c)), Seq(s.variance))
    }

    registerTest(s"$name - count only") {
      val (df, c) = wrapped()
      compare(df.select(metrics("count").summary(c), count(c)),
        Seq(Row(exp.count), exp.count))
    }

    registerTest(s"$name - count only (direct)") {
      val (df, c) = wrapped()
      compare(df.select(count(c)),
        Seq(exp.count))
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
    val coll = df.collect().toSeq
    val Seq(row) = coll
    val res = row.toSeq
    val names = df.schema.fieldNames.zipWithIndex.map { case (n, idx) => s"$n ($idx)" }
    assert(res.size === exp.size, (res.size, exp.size))
    for (((x1, x2), name) <- res.zip(exp).zip(names)) {
      compareStructures(x1, x2, name)
    }
  }

  // Compares structured content.
  private def compareStructures(x1: Any, x2: Any, name: String): Unit = (x1, x2) match {
    case (y1: Seq[Double @unchecked], v1: OldVector) =>
      compareStructures(y1, v1.toArray.toSeq, name)
    case (d1: Double, d2: Double) =>
      assert2(Vectors.dense(d1) ~== Vectors.dense(d2) absTol 1e-4, name)
    case (r1: GenericRowWithSchema, r2: Row) =>
      assert(r1.size === r2.size, (r1, r2))
      for (((fname, x1), x2) <- r1.schema.fieldNames.zip(r1.toSeq).zip(r2.toSeq)) {
        compareStructures(x1, x2, s"$name.$fname")
      }
    case (r1: Row, r2: Row) =>
      assert(r1.size === r2.size, (r1, r2))
      for ((x1, x2) <- r1.toSeq.zip(r2.toSeq)) { compareStructures(x1, x2, name) }
    case (v1: Vector, v2: Vector) =>
      assert2(v1 ~== v2 absTol 1e-4, name)
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

  private def assert2(x: => Boolean, hint: String): Unit = {
    try {
      assert(x, hint)
    } catch {
      case tfe: TestFailedException =>
        throw new TestFailedException(Some(s"Failure with hint $hint"), Some(tfe), 1)
    }
  }

  private def makeBuffer(vecs: Seq[Vector]): Buffer = {
    val b = Buffer.allMetrics()
    for (v <- vecs) { Buffer.updateInPlace(b, v, 1.0) }
    b
  }

  private def b(x: Array[Double]): Vector = Vectors.dense(x)

  private def l(x: Array[Long]): Vector = b(x.map(_.toDouble))

  test("debugging test") {
    val df = denseData(Nil)
    val c = df.col("features")
    val c1 = metrics("mean").summary(c)
    val res = df.select(c1)
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
    // TODO: I have a doubt about these values, they are not normalized.
    variance = Seq(0.0, 2.0, 8.0),
    count = 2,
    numNonZeros = Seq(0, 2, 2),
    max = Seq(0.0, 1.0, 2.0),
    min = Seq(0.0, -1.0, -2.0),
    normL1 = Seq(0.0, 2.0, 4.0),
    normL2 = Seq(0.0, math.sqrt(2.0), math.sqrt(2.0) * 2.0)
  ))

  testExample("dense vector input",
    Seq(Seq(-1.0, 0.0, 6.0), Seq(3.0, -3.0, 0.0)),
    ExpectedMetrics(
      mean = Seq(1.0, -1.5, 3.0),
      variance = Seq(8.0, 4.5, 18.0),
      count = 2,
      numNonZeros = Seq(2, 1, 1),
      max = Seq(3.0, 0.0, 6.0),
      min = Seq(-1.0, -3, 0.0),
      normL1 = Seq(4.0, 3.0, 6.0),
      normL2 = Seq(math.sqrt(10), 3, 6.0)
  ))

  test("mixing dense and sparse vector input") {
    val summarizer = makeBuffer(Seq(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(0.0, -1.0, -3.0),
      Vectors.sparse(3, Seq((1, -5.1))),
      Vectors.dense(3.8, 0.0, 1.9),
      Vectors.dense(1.7, -0.6, 0.0),
      Vectors.sparse(3, Seq((1, 1.9), (2, 0.0)))))

    assert(b(Buffer.mean(summarizer)) ~==
      Vectors.dense(0.583333333333, -0.416666666666, -0.183333333333) absTol 1E-5, "mean mismatch")

    assert(b(Buffer.min(summarizer)) ~== Vectors.dense(-2.0, -5.1, -3) absTol 1E-5, "min " +
      "mismatch")

    assert(b(Buffer.max(summarizer)) ~== Vectors.dense(3.8, 2.3, 1.9) absTol 1E-5, "max mismatch")

    assert(l(Buffer.nnz(summarizer)) ~== Vectors.dense(3, 5, 2) absTol 1E-5, "numNonzeros mismatch")

    assert(b(Buffer.variance(summarizer)) ~==
      Vectors.dense(3.857666666666, 7.0456666666666, 2.48166666666666) absTol 1E-5,
      "variance mismatch")

    assert(Buffer.totalCount(summarizer) === 6)
  }


  test("merging two summarizers") {
    val summarizer1 = makeBuffer(Seq(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(0.0, -1.0, -3.0)))

    val summarizer2 = makeBuffer(Seq(
      Vectors.sparse(3, Seq((1, -5.1))),
      Vectors.dense(3.8, 0.0, 1.9),
      Vectors.dense(1.7, -0.6, 0.0),
      Vectors.sparse(3, Seq((1, 1.9), (2, 0.0)))))

    val summarizer = Buffer.mergeBuffers(summarizer1, summarizer2)

    assert(b(Buffer.mean(summarizer)) ~==
      Vectors.dense(0.583333333333, -0.416666666666, -0.183333333333) absTol 1E-5, "mean mismatch")

    assert(b(Buffer.min(summarizer)) ~== Vectors.dense(-2.0, -5.1, -3) absTol 1E-5, "min mismatch")

    assert(b(Buffer.max(summarizer)) ~== Vectors.dense(3.8, 2.3, 1.9) absTol 1E-5, "max mismatch")

    assert(l(Buffer.nnz(summarizer)) ~== Vectors.dense(3, 5, 2) absTol 1E-5, "numNonzeros mismatch")

    assert(b(Buffer.variance(summarizer)) ~==
      Vectors.dense(3.857666666666, 7.0456666666666, 2.48166666666666) absTol 1E-5,
      "variance mismatch")

    assert(Buffer.totalCount(summarizer) === 6)
  }

  // TODO: this test should not be committed. It is here to isolate some performance hotspots.
  test("perf test") {
    val n = 10000000
    val rdd1 = sc.parallelize(1 to n).map { idx =>
      OldVectors.dense(idx.toDouble)
    }
    val trieouts = 100
    rdd1.cache()
    rdd1.count()
    val rdd2 = sc.parallelize(1 to n).map { idx =>
      Vectors.dense(idx.toDouble)
    }
    rdd2.cache()
    rdd2.count()
    val df = rdd2.map(Tuple1.apply).toDF("features")
    df.cache()
    df.count()
    val x = df.select(
      metrics("mean", "variance", "count", "numNonZeros", "max", "min", "normL1",
              "normL2").summary($"features"))
    val x1 = df.select(metrics("variance").summary($"features"))

    def print(name: String, l: List[Long]): Unit = {
      def f(z: Long) = (1e6 * n.toDouble) / z
      val min = f(l.max)
      val max = f(l.min)
      val med = f(l.sorted.drop(l.size / 2).head)

      // scalastyle:off println
      println(s"$name = [$min ~ $med ~ $max] records / milli")
    }


    var times_df: List[Long] = Nil
    for (_ <- 1 to trieouts) {
      System.gc()
      val t21 = System.nanoTime()
      x.head()
      val t22 = System.nanoTime()
      val dt = t22 - t21
      times_df ::= dt
      // scalastyle:off
      print("Dataframes", times_df)
      // scalastyle:on
    }

    var times_rdd: List[Long] = Nil
//    for (_ <- 1 to trieouts) {
//      val t21 = System.nanoTime()
//      Statistics.colStats(rdd1)
//      val t22 = System.nanoTime()
//      times_rdd ::= (t22 - t21)
//    }

    var times_df_variance: List[Long] = Nil
//    for (_ <- 1 to trieouts) {
//      val t21 = System.nanoTime()
//      x1.head()
//      val t22 = System.nanoTime()
//      times_df_variance ::= (t22 - t21)
//    }


    print("Dataframes", times_df)
    print("RDD", times_rdd)
    print("Dataframes (variance only)", times_df_variance)
  }

}
