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
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, Statistics}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class SummarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._
  import Summarizer._
  import SummaryBuilderImpl._

  private case class ExpectedMetrics(
      mean: Seq[Double],
      variance: Seq[Double],
      count: Long,
      numNonZeros: Seq[Long],
      max: Seq[Double],
      min: Seq[Double],
      normL2: Seq[Double],
      normL1: Seq[Double])

  /**
   * The input is expected to be either a sparse vector, a dense vector or an array of doubles
   * (which will be converted to a dense vector)
   * The expected is the list of all the known metrics.
   *
   * The tests take an list of input vectors and a list of all the summary values that
   * are expected for this input. They currently test against some fixed subset of the
   * metrics, but should be made fuzzy in the future.
   */
  private def testExample(name: String, input: Seq[Any], exp: ExpectedMetrics): Unit = {

    def inputVec: Seq[Vector] = input.map {
      case x: Array[Double @unchecked] => Vectors.dense(x)
      case x: Seq[Double @unchecked] => Vectors.dense(x.toArray)
      case x: Vector => x
      case x => throw new Exception(x.toString)
    }

    val summarizer = {
      val _summarizer = new MultivariateOnlineSummarizer
      inputVec.foreach(v => _summarizer.add(OldVectors.fromML(v)))
      _summarizer
    }

    // Because the Spark context is reset between tests, we cannot hold a reference onto it.
    def wrappedInit() = {
      val df = inputVec.map(Tuple1.apply).toDF("features")
      val col = df.col("features")
      (df, col)
    }

    registerTest(s"$name - mean only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("mean").summary(c), mean(c)), Seq(Row(exp.mean), summarizer.mean))
    }

    registerTest(s"$name - mean only (direct)") {
      val (df, c) = wrappedInit()
      compare(df.select(mean(c)), Seq(exp.mean))
    }

    registerTest(s"$name - variance only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("variance").summary(c), variance(c)),
        Seq(Row(exp.variance), summarizer.variance))
    }

    registerTest(s"$name - variance only (direct)") {
      val (df, c) = wrappedInit()
      compare(df.select(variance(c)), Seq(summarizer.variance))
    }

    registerTest(s"$name - count only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("count").summary(c), count(c)),
        Seq(Row(exp.count), exp.count))
    }

    registerTest(s"$name - count only (direct)") {
      val (df, c) = wrappedInit()
      compare(df.select(count(c)),
        Seq(exp.count))
    }

    registerTest(s"$name - numNonZeros only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("numNonZeros").summary(c), numNonZeros(c)),
        Seq(Row(exp.numNonZeros), exp.numNonZeros))
    }

    registerTest(s"$name - numNonZeros only (direct)") {
      val (df, c) = wrappedInit()
      compare(df.select(numNonZeros(c)),
        Seq(exp.numNonZeros))
    }

    registerTest(s"$name - min only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("min").summary(c), min(c)),
        Seq(Row(exp.min), exp.min))
    }

    registerTest(s"$name - max only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("max").summary(c), max(c)),
        Seq(Row(exp.max), exp.max))
    }

    registerTest(s"$name - normL1 only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("normL1").summary(c), normL1(c)),
        Seq(Row(exp.normL1), exp.normL1))
    }

    registerTest(s"$name - normL2 only") {
      val (df, c) = wrappedInit()
      compare(df.select(metrics("normL2").summary(c), normL2(c)),
        Seq(Row(exp.normL2), exp.normL2))
    }

    registerTest(s"$name - all metrics at once") {
      val (df, c) = wrappedInit()
      compare(df.select(
        metrics("mean", "variance", "count", "numNonZeros").summary(c),
        mean(c), variance(c), count(c), numNonZeros(c)),
        Seq(Row(exp.mean, exp.variance, exp.count, exp.numNonZeros),
          exp.mean, exp.variance, exp.count, exp.numNonZeros))
    }
  }

  private def denseData(input: Seq[Seq[Double]]): DataFrame = {
    input.map(_.toArray).map(Vectors.dense).map(Tuple1.apply).toDF("features")
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

  val singleElem = Seq(0.0, 1.0, 2.0)
  testExample("single element", Seq(singleElem), ExpectedMetrics(
    mean = singleElem,
    variance = Seq(0.0, 0.0, 0.0),
    count = 1,
    numNonZeros = Seq(0, 1, 1),
    max = singleElem,
    min = singleElem,
    normL1 = singleElem,
    normL2 = singleElem
  ))

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
    )
  )

  test("summarizer buffer basic error handing") {
    val summarizer = new SummarizerBuffer

    assert(summarizer.count === 0, "should be zero since nothing is added.")

    withClue("Getting numNonzeros from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.numNonzeros
      }
    }

    withClue("Getting variance from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.variance
      }
    }

    withClue("Getting mean from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.mean
      }
    }

    withClue("Getting max from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.max
      }
    }

    withClue("Getting min from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.min
      }
    }

    summarizer.add(Vectors.dense(-1.0, 2.0, 6.0)).add(Vectors.sparse(3, Seq((0, -2.0), (1, 6.0))))

    withClue("Adding a new dense sample with different array size should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.add(Vectors.dense(3.0, 1.0))
      }
    }

    withClue("Adding a new sparse sample with different array size should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.add(Vectors.sparse(5, Seq((0, -2.0), (1, 6.0))))
      }
    }

    val summarizer2 = (new SummarizerBuffer).add(Vectors.dense(1.0, -2.0, 0.0, 4.0))
    withClue("Merging a new summarizer with different dimensions should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.merge(summarizer2)
      }
    }
  }

  test("summarizer buffer dense vector input") {
    // For column 2, the maximum will be 0.0, and it's not explicitly added since we ignore all
    // the zeros; it's a case we need to test. For column 3, the minimum will be 0.0 which we
    // need to test as well.
    val summarizer = (new SummarizerBuffer)
      .add(Vectors.dense(-1.0, 0.0, 6.0))
      .add(Vectors.dense(3.0, -3.0, 0.0))

    assert(summarizer.mean ~== Vectors.dense(1.0, -1.5, 3.0) absTol 1E-5, "mean mismatch")
    assert(summarizer.min ~== Vectors.dense(-1.0, -3, 0.0) absTol 1E-5, "min mismatch")
    assert(summarizer.max ~== Vectors.dense(3.0, 0.0, 6.0) absTol 1E-5, "max mismatch")
    assert(summarizer.numNonzeros ~== Vectors.dense(2, 1, 1) absTol 1E-5, "numNonzeros mismatch")
    assert(summarizer.variance ~== Vectors.dense(8.0, 4.5, 18.0) absTol 1E-5, "variance mismatch")
    assert(summarizer.count === 2)
  }

  test("summarizer buffer sparse vector input") {
    val summarizer = (new SummarizerBuffer)
      .add(Vectors.sparse(3, Seq((0, -1.0), (2, 6.0))))
      .add(Vectors.sparse(3, Seq((0, 3.0), (1, -3.0))))

    assert(summarizer.mean ~== Vectors.dense(1.0, -1.5, 3.0) absTol 1E-5, "mean mismatch")
    assert(summarizer.min ~== Vectors.dense(-1.0, -3, 0.0) absTol 1E-5, "min mismatch")
    assert(summarizer.max ~== Vectors.dense(3.0, 0.0, 6.0) absTol 1E-5, "max mismatch")
    assert(summarizer.numNonzeros ~== Vectors.dense(2, 1, 1) absTol 1E-5, "numNonzeros mismatch")
    assert(summarizer.variance ~== Vectors.dense(8.0, 4.5, 18.0) absTol 1E-5, "variance mismatch")
    assert(summarizer.count === 2)
  }

  test("summarizer buffer mixing dense and sparse vector input") {
    val summarizer = (new SummarizerBuffer)
      .add(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))))
      .add(Vectors.dense(0.0, -1.0, -3.0))
      .add(Vectors.sparse(3, Seq((1, -5.1))))
      .add(Vectors.dense(3.8, 0.0, 1.9))
      .add(Vectors.dense(1.7, -0.6, 0.0))
      .add(Vectors.sparse(3, Seq((1, 1.9), (2, 0.0))))

    assert(summarizer.mean ~==
      Vectors.dense(0.583333333333, -0.416666666666, -0.183333333333) absTol 1E-5, "mean mismatch")

    assert(summarizer.min ~== Vectors.dense(-2.0, -5.1, -3) absTol 1E-5, "min mismatch")
    assert(summarizer.max ~== Vectors.dense(3.8, 2.3, 1.9) absTol 1E-5, "max mismatch")
    assert(summarizer.numNonzeros ~== Vectors.dense(3, 5, 2) absTol 1E-5, "numNonzeros mismatch")
    assert(summarizer.variance ~==
      Vectors.dense(3.857666666666, 7.0456666666666, 2.48166666666666) absTol 1E-5,
      "variance mismatch")

    assert(summarizer.count === 6)
  }

  test("summarizer buffer merging two summarizers") {
    val summarizer1 = (new SummarizerBuffer)
      .add(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))))
      .add(Vectors.dense(0.0, -1.0, -3.0))

    val summarizer2 = (new SummarizerBuffer)
      .add(Vectors.sparse(3, Seq((1, -5.1))))
      .add(Vectors.dense(3.8, 0.0, 1.9))
      .add(Vectors.dense(1.7, -0.6, 0.0))
      .add(Vectors.sparse(3, Seq((1, 1.9), (2, 0.0))))

    val summarizer = summarizer1.merge(summarizer2)

    assert(summarizer.mean ~==
      Vectors.dense(0.583333333333, -0.416666666666, -0.183333333333) absTol 1E-5, "mean mismatch")

    assert(summarizer.min ~== Vectors.dense(-2.0, -5.1, -3) absTol 1E-5, "min mismatch")
    assert(summarizer.max ~== Vectors.dense(3.8, 2.3, 1.9) absTol 1E-5, "max mismatch")
    assert(summarizer.numNonzeros ~== Vectors.dense(3, 5, 2) absTol 1E-5, "numNonzeros mismatch")
    assert(summarizer.variance ~==
      Vectors.dense(3.857666666666, 7.0456666666666, 2.48166666666666) absTol 1E-5,
      "variance mismatch")
    assert(summarizer.count === 6)
  }

  test("summarizer buffer zero variance test (SPARK-21818)") {
    val summarizer1 = new SummarizerBuffer()
      .add(Vectors.dense(3.0), 0.7)
    val summarizer2 = new SummarizerBuffer()
      .add(Vectors.dense(3.0), 0.4)
    val summarizer3 = new SummarizerBuffer()
      .add(Vectors.dense(3.0), 0.5)
    val summarizer4 = new SummarizerBuffer()
      .add(Vectors.dense(3.0), 0.4)

    val summarizer = summarizer1
      .merge(summarizer2)
      .merge(summarizer3)
      .merge(summarizer4)

    assert(summarizer.variance(0) >= 0.0)
  }

  test("summarizer buffer merging summarizer with empty summarizer") {
    // If one of two is non-empty, this should return the non-empty summarizer.
    // If both of them are empty, then just return the empty summarizer.
    val summarizer1 = (new SummarizerBuffer)
      .add(Vectors.dense(0.0, -1.0, -3.0)).merge(new SummarizerBuffer)
    assert(summarizer1.count === 1)

    val summarizer2 = (new SummarizerBuffer)
      .merge((new SummarizerBuffer).add(Vectors.dense(0.0, -1.0, -3.0)))
    assert(summarizer2.count === 1)

    val summarizer3 = (new SummarizerBuffer).merge(new SummarizerBuffer)
    assert(summarizer3.count === 0)

    assert(summarizer1.mean ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "mean mismatch")
    assert(summarizer2.mean ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "mean mismatch")
    assert(summarizer1.min ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "min mismatch")
    assert(summarizer2.min ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "min mismatch")
    assert(summarizer1.max ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "max mismatch")
    assert(summarizer2.max ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "max mismatch")
    assert(summarizer1.numNonzeros ~== Vectors.dense(0, 1, 1) absTol 1E-5, "numNonzeros mismatch")
    assert(summarizer2.numNonzeros ~== Vectors.dense(0, 1, 1) absTol 1E-5, "numNonzeros mismatch")
    assert(summarizer1.variance ~== Vectors.dense(0, 0, 0) absTol 1E-5, "variance mismatch")
    assert(summarizer2.variance ~== Vectors.dense(0, 0, 0) absTol 1E-5, "variance mismatch")
  }

  test("summarizer buffer merging summarizer when one side has zero mean (SPARK-4355)") {
    val s0 = new SummarizerBuffer()
      .add(Vectors.dense(2.0))
      .add(Vectors.dense(2.0))
    val s1 = new SummarizerBuffer()
      .add(Vectors.dense(1.0))
      .add(Vectors.dense(-1.0))
    s0.merge(s1)
    assert(s0.mean(0) ~== 1.0 absTol 1e-14)
  }

  test("summarizer buffer merging summarizer with weighted samples") {
    val summarizer = (new SummarizerBuffer)
      .add(Vectors.sparse(3, Seq((0, -0.8), (1, 1.7))), weight = 0.1)
      .add(Vectors.dense(0.0, -1.2, -1.7), 0.2).merge(
      (new SummarizerBuffer)
        .add(Vectors.sparse(3, Seq((0, -0.7), (1, 0.01), (2, 1.3))), 0.15)
        .add(Vectors.dense(-0.5, 0.3, -1.5), 0.05))

    assert(summarizer.count === 4)

    // The following values are hand calculated using the formula:
    // [[https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Reliability_weights]]
    // which defines the reliability weight used for computing the unbiased estimation of variance
    // for weighted instances.
    assert(summarizer.mean ~== Vectors.dense(Array(-0.42, -0.107, -0.44))
      absTol 1E-10, "mean mismatch")
    assert(summarizer.variance ~== Vectors.dense(Array(0.17657142857, 1.645115714, 2.42057142857))
      absTol 1E-8, "variance mismatch")
    assert(summarizer.numNonzeros ~== Vectors.dense(Array(3.0, 4.0, 3.0))
      absTol 1E-10, "numNonzeros mismatch")
    assert(summarizer.max ~== Vectors.dense(Array(0.0, 1.7, 1.3)) absTol 1E-10, "max mismatch")
    assert(summarizer.min ~== Vectors.dense(Array(-0.8, -1.2, -1.7)) absTol 1E-10, "min mismatch")
    assert(summarizer.normL2 ~== Vectors.dense(0.387298335, 0.762571308141, 0.9715966241192)
      absTol 1E-8, "normL2 mismatch")
    assert(summarizer.normL1 ~== Vectors.dense(0.21, 0.4265, 0.61) absTol 1E-10, "normL1 mismatch")
  }

  test("summarizer buffer test min/max with weighted samples") {
    val summarizer1 = new SummarizerBuffer()
      .add(Vectors.dense(10.0, -10.0), 1e10)
      .add(Vectors.dense(0.0, 0.0), 1e-7)

    val summarizer2 = new SummarizerBuffer()
    summarizer2.add(Vectors.dense(10.0, -10.0), 1e10)
    for (i <- 1 to 100) {
      summarizer2.add(Vectors.dense(0.0, 0.0), 1e-7)
    }

    val summarizer3 = new SummarizerBuffer()
    for (i <- 1 to 100) {
      summarizer3.add(Vectors.dense(0.0, 0.0), 1e-7)
    }
    summarizer3.add(Vectors.dense(10.0, -10.0), 1e10)

    assert(summarizer1.max ~== Vectors.dense(10.0, 0.0) absTol 1e-14)
    assert(summarizer1.min ~== Vectors.dense(0.0, -10.0) absTol 1e-14)
    assert(summarizer2.max ~== Vectors.dense(10.0, 0.0) absTol 1e-14)
    assert(summarizer2.min ~== Vectors.dense(0.0, -10.0) absTol 1e-14)
    assert(summarizer3.max ~== Vectors.dense(10.0, 0.0) absTol 1e-14)
    assert(summarizer3.min ~== Vectors.dense(0.0, -10.0) absTol 1e-14)
  }

  ignore("performance test") {
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.12
    MacBook Pro (15-inch, 2016) CPU 2.9 GHz Intel Core i7
    Use 2 partitions. tries out times= 20, warm up times = 10

    The unit of test results is records/milliseconds (higher is better)

    Vector size/records number:     1/1E7    10/1E6   100/1E6   1E3/1E5   1E4/1E4
    -----------------------------------------------------------------------------
    DataFrame                       15149      7441      2118       224        21
    RDD from DataFrame               4992      4440      2328       320        33
    Raw RDD                         53931     20683      3966       528        53
    */
    import scala.util.Random
    val rand = new Random()

    val genArr = (dim: Int) => {
      Array.fill(dim)(rand.nextDouble())
    }

    val numPartitions = 2
    for ( (n, dim) <- Seq(
      (10000000, 1), (1000000, 10), (1000000, 100), (100000, 1000), (10000, 10000))
    ) {
      val rdd1 = sc.parallelize(1 to n, numPartitions).map { idx =>
        OldVectors.dense(genArr(dim))
      }
      // scalastyle:off println
      println(s"records number = $n, vector size = $dim, partition = ${rdd1.getNumPartitions}")
      // scalastyle:on println

      val numOfTry = 20
      val numOfWarmUp = 10
      rdd1.cache()
      rdd1.count()
      val rdd2 = sc.parallelize(1 to n, numPartitions).map { idx =>
        Vectors.dense(genArr(dim))
      }
      rdd2.cache()
      rdd2.count()
      val df = rdd2.map(Tuple1.apply).toDF("features")
      df.cache()
      df.count()

      def print(name: String, l: List[Long]): Unit = {
        def f(z: Long) = (1e6 * n.toDouble) / z
        val min = f(l.max)
        val max = f(l.min)
        val med = f(l.sorted.drop(l.size / 2).head)
        // scalastyle:off println
        println(s"$name = [$min ~ $med ~ $max] records / milli")
        // scalastyle:on println
      }

      var timeDF: List[Long] = Nil
      val x = df.select(
        metrics("mean", "variance", "count", "numNonZeros", "max", "min", "normL1",
          "normL2").summary($"features"))
      for (i <- 1 to numOfTry) {
        val start = System.nanoTime()
        x.head()
        val end = System.nanoTime()
        if (i > numOfWarmUp) timeDF ::= (end - start)
      }

      var timeRDD: List[Long] = Nil
      for (i <- 1 to numOfTry) {
        val start = System.nanoTime()
        Statistics.colStats(rdd1)
        val end = System.nanoTime()
        if (i > numOfWarmUp) timeRDD ::= (end - start)
      }

      var timeRDDFromDF: List[Long] = Nil
      val rddFromDf = df.rdd.map { case Row(v: Vector) => OldVectors.fromML(v) }
      for (i <- 1 to numOfTry) {
        val start = System.nanoTime()
        Statistics.colStats(rddFromDf)
        val end = System.nanoTime()
        if (i > numOfWarmUp) timeRDDFromDF ::= (end - start)
      }

      print("DataFrame : ", timeDF)
      print("RDD :", timeRDD)
      print("RDD from DataFrame : ", timeRDDFromDF)
    }
  }
}
