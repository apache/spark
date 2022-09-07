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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, Statistics}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class SummarizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._
  import Summarizer._

  private case class ExpectedMetrics(
      mean: Vector,
      variance: Vector,
      count: Long,
      numNonZeros: Vector,
      max: Vector,
      min: Vector,
      normL2: Vector,
      normL1: Vector)

  /**
   * The input is expected to be either a sparse vector, a dense vector.
   *
   * The tests take an list of input vectors, and compare results with
   * `mllib.stat.MultivariateOnlineSummarizer`. They currently test against some fixed subset
   * of the metrics, but should be made fuzzy in the future.
   */
  private def testExample(name: String, inputVec: Seq[(Vector, Double)],
      exp: ExpectedMetrics, expWithoutWeight: ExpectedMetrics): Unit = {

    val summarizer = {
      val _summarizer = new MultivariateOnlineSummarizer
      inputVec.foreach(v => _summarizer.add(OldVectors.fromML(v._1), v._2))
      _summarizer
    }

    val summarizerWithoutWeight = {
      val _summarizer = new MultivariateOnlineSummarizer
      inputVec.foreach(v => _summarizer.add(OldVectors.fromML(v._1)))
      _summarizer
    }

    // Because the Spark context is reset between tests, we cannot hold a reference onto it.
    def wrappedInit() = {
      val df = inputVec.toDF("features", "weight")
      val featuresCol = df.col("features")
      val weightCol = df.col("weight")
      (df, featuresCol, weightCol)
    }

    registerTest(s"$name - mean only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("mean").summary(c, w), mean(c, w)).first(),
        Row(Row(summarizer.mean), exp.mean))
    }

    registerTest(s"$name - mean only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("mean").summary(c), mean(c)).first(),
        Row(Row(summarizerWithoutWeight.mean), expWithoutWeight.mean))
    }

    registerTest(s"$name - sum only") {
      val (df, c, w) = wrappedInit()
      val weightSum = summarizer.weightSum
      val expected1 = summarizer.mean.asML.copy
      BLAS.scal(weightSum, expected1)
      val expected2 = exp.mean.copy
      BLAS.scal(weightSum, expected2)
      compareRow(df.select(metrics("sum").summary(c, w), sum(c, w)).first(),
        Row(Row(expected1), expected2))
    }

    registerTest(s"$name - sum only w/o weight") {
      val (df, c, _) = wrappedInit()
      val weightSum = summarizerWithoutWeight.weightSum
      val expected1 = summarizerWithoutWeight.mean.asML.copy
      BLAS.scal(weightSum, expected1)
      val expected2 = expWithoutWeight.mean.copy
      BLAS.scal(weightSum, expected2)
      compareRow(df.select(metrics("sum").summary(c), sum(c)).first(),
        Row(Row(expected1), expected2))
    }

    registerTest(s"$name - variance only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("variance").summary(c, w), variance(c, w)).first(),
        Row(Row(summarizer.variance), exp.variance))
    }

    registerTest(s"$name - variance only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("variance").summary(c), variance(c)).first(),
        Row(Row(summarizerWithoutWeight.variance), expWithoutWeight.variance))
    }

    registerTest(s"$name - std only") {
      val (df, c, w) = wrappedInit()
      val expected1 = Vectors.dense(summarizer.variance.toArray.map(math.sqrt))
      val expected2 = Vectors.dense(exp.variance.toArray.map(math.sqrt))
      compareRow(df.select(metrics("std").summary(c, w), std(c, w)).first(),
        Row(Row(expected1), expected2))
    }

    registerTest(s"$name - std only w/o weight") {
      val (df, c, _) = wrappedInit()
      val expected1 = Vectors.dense(summarizerWithoutWeight.variance.toArray.map(math.sqrt))
      val expected2 = Vectors.dense(expWithoutWeight.variance.toArray.map(math.sqrt))
      compareRow(df.select(metrics("std").summary(c), std(c)).first(),
        Row(Row(expected1), expected2))
    }

    registerTest(s"$name - count only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("count").summary(c, w), count(c, w)).first(),
        Row(Row(summarizer.count), exp.count))
    }

    registerTest(s"$name - count only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("count").summary(c), count(c)).first(),
        Row(Row(summarizerWithoutWeight.count), expWithoutWeight.count))
    }

    registerTest(s"$name - numNonZeros only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("numNonZeros").summary(c, w), numNonZeros(c, w)).first(),
        Row(Row(summarizer.numNonzeros), exp.numNonZeros))
    }

    registerTest(s"$name - numNonZeros only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("numNonZeros").summary(c), numNonZeros(c)).first(),
        Row(Row(summarizerWithoutWeight.numNonzeros), expWithoutWeight.numNonZeros))
    }

    registerTest(s"$name - min only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("min").summary(c, w), min(c, w)).first(),
        Row(Row(summarizer.min), exp.min))
    }

    registerTest(s"$name - min only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("min").summary(c), min(c)).first(),
        Row(Row(summarizerWithoutWeight.min), expWithoutWeight.min))
    }

    registerTest(s"$name - max only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("max").summary(c, w), max(c, w)).first(),
        Row(Row(summarizer.max), exp.max))
    }

    registerTest(s"$name - max only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("max").summary(c), max(c)).first(),
        Row(Row(summarizerWithoutWeight.max), expWithoutWeight.max))
    }

    registerTest(s"$name - normL1 only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("normL1").summary(c, w), normL1(c, w)).first(),
        Row(Row(summarizer.normL1), exp.normL1))
    }

    registerTest(s"$name - normL1 only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("normL1").summary(c), normL1(c)).first(),
        Row(Row(summarizerWithoutWeight.normL1), expWithoutWeight.normL1))
    }

    registerTest(s"$name - normL2 only") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(metrics("normL2").summary(c, w), normL2(c, w)).first(),
        Row(Row(summarizer.normL2), exp.normL2))
    }

    registerTest(s"$name - normL2 only w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(metrics("normL2").summary(c), normL2(c)).first(),
        Row(Row(summarizerWithoutWeight.normL2), expWithoutWeight.normL2))
    }

    registerTest(s"$name - multiple metrics at once") {
      val (df, c, w) = wrappedInit()
      compareRow(df.select(
        metrics("mean", "variance", "count", "numNonZeros").summary(c, w)).first(),
        Row(Row(exp.mean, exp.variance, exp.count, exp.numNonZeros))
      )
    }

    registerTest(s"$name - multiple metrics at once w/o weight") {
      val (df, c, _) = wrappedInit()
      compareRow(df.select(
        metrics("mean", "variance", "count", "numNonZeros").summary(c)).first(),
        Row(Row(expWithoutWeight.mean, expWithoutWeight.variance,
          expWithoutWeight.count, expWithoutWeight.numNonZeros))
      )
    }
  }

  private def compareRow(r1: Row, r2: Row): Unit = {
    assert(r1.size === r2.size, (r1, r2))
    r1.toSeq.zip(r2.toSeq).foreach {
      case (v1: Vector, v2: Vector) =>
        assert(v1 ~== v2 absTol 1e-4)
      case (v1: Vector, v2: OldVector) =>
        assert(v1 ~== v2.asML absTol 1e-4)
      case (i1: Int, i2: Int) =>
        assert(i1 === i2)
      case (l1: Long, l2: Long) =>
        assert(l1 === l2)
      case (d1: Double, d2: Double) =>
        assert(d1 ~== d2 absTol 1e-4)
      case (r1: Row, r2: Row) =>
        compareRow(r1, r2)
      case (x1: Any, x2: Any) =>
        throw new Exception(s"type mismatch: ${x1.getClass} ${x2.getClass} $x1 $x2")
    }
  }

  test("no element") {
    val df = Seq[Tuple1[Vector]]().toDF("features")
    val c = df.col("features")
    intercept[SparkException] {
      df.select(metrics("mean").summary(c), mean(c)).first()
    }
    compareRow(df.select(metrics("count").summary(c), count(c)).first(),
      Row(Row(0L), 0L))
  }

  val singleElem = Vectors.dense(0.0, 1.0, 2.0)
  testExample("single element", Seq((singleElem, 2.0)),
    ExpectedMetrics(
      mean = singleElem,
      variance = Vectors.dense(0.0, 0.0, 0.0),
      count = 1L,
      numNonZeros = Vectors.dense(0.0, 1.0, 1.0),
      max = singleElem,
      min = singleElem,
      normL1 = Vectors.dense(0.0, 2.0, 4.0),
      normL2 = Vectors.dense(0.0, 1.414213, 2.828427)
    ),
    ExpectedMetrics(
      mean = singleElem,
      variance = Vectors.dense(0.0, 0.0, 0.0),
      count = 1L,
      numNonZeros = Vectors.dense(0.0, 1.0, 1.0),
      max = singleElem,
      min = singleElem,
      normL1 = singleElem,
      normL2 = singleElem
    )
  )

  testExample("multiple elements (dense)",
    Seq(
      (Vectors.dense(-1.0, 0.0, 6.0), 0.5),
      (Vectors.dense(3.0, -3.0, 0.0), 2.8),
      (Vectors.dense(1.0, -3.0, 0.0), 0.0)
    ),
    ExpectedMetrics(
      mean = Vectors.dense(2.393939, -2.545454, 0.909090),
      variance = Vectors.dense(8.0, 4.5, 18.0),
      count = 2L,
      numNonZeros = Vectors.dense(2.0, 1.0, 1.0),
      max = Vectors.dense(3.0, 0.0, 6.0),
      min = Vectors.dense(-1.0, -3.0, 0.0),
      normL1 = Vectors.dense(8.9, 8.4, 3.0),
      normL2 = Vectors.dense(5.069516, 5.019960, 4.242640)
    ),
    ExpectedMetrics(
      mean = Vectors.dense(1.0, -2.0, 2.0),
      variance = Vectors.dense(4.0, 3.0, 12.0),
      count = 3L,
      numNonZeros = Vectors.dense(3.0, 2.0, 1.0),
      max = Vectors.dense(3.0, 0.0, 6.0),
      min = Vectors.dense(-1.0, -3.0, 0.0),
      normL1 = Vectors.dense(5.0, 6.0, 6.0),
      normL2 = Vectors.dense(3.316624, 4.242640, 6.0)
    )
  )

  testExample("multiple elements (sparse)",
    Seq(
      (Vectors.dense(-1.0, 0.0, 6.0).toSparse, 0.5),
      (Vectors.dense(3.0, -3.0, 0.0).toSparse, 2.8),
      (Vectors.dense(1.0, -3.0, 0.0).toSparse, 0.0)
    ),
    ExpectedMetrics(
      mean = Vectors.dense(2.393939, -2.545454, 0.909090),
      variance = Vectors.dense(8.0, 4.5, 18.0),
      count = 2L,
      numNonZeros = Vectors.dense(2.0, 1.0, 1.0),
      max = Vectors.dense(3.0, 0.0, 6.0),
      min = Vectors.dense(-1.0, -3.0, 0.0),
      normL1 = Vectors.dense(8.9, 8.4, 3.0),
      normL2 = Vectors.dense(5.069516, 5.019960, 4.242640)
    ),
    ExpectedMetrics(
      mean = Vectors.dense(1.0, -2.0, 2.0),
      variance = Vectors.dense(4.0, 3.0, 12.0),
      count = 3L,
      numNonZeros = Vectors.dense(3.0, 2.0, 1.0),
      max = Vectors.dense(3.0, 0.0, 6.0),
      min = Vectors.dense(-1.0, -3.0, 0.0),
      normL1 = Vectors.dense(5.0, 6.0, 6.0),
      normL2 = Vectors.dense(3.316624, 4.242640, 6.0)
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

  test("support new metrics: sum, std, numFeatures, sumL2, weightSum") {
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

    Seq(summarizer1, summarizer2, summarizer3).foreach { summarizer =>
      val variance = summarizer.variance
      val expectedStd = Vectors.dense(variance.toArray.map(math.sqrt))
      assert(summarizer.std ~== expectedStd relTol 1e-14)
    }
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
