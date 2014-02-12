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

package org.apache.spark.mllib.util

import java.io.File

import org.scalatest.FunSuite

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, norm => breezeNorm,
  squaredDistance => breezeSquaredDistance}
import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils._

class MLUtilsSuite extends FunSuite with LocalSparkContext {

  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("epsilon computation") {
    assert(1.0 + EPSILON > 1.0, s"EPSILON is too small: $EPSILON.")
    assert(1.0 + EPSILON / 2.0 === 1.0, s"EPSILON is too big: $EPSILON.")
  }

  test("fast squared distance") {
    val a = (30 to 0 by -1).map(math.pow(2.0, _)).toArray
    val n = a.length
    val v1 = new BDV[Double](a)
    val norm1 = breezeNorm(v1, 2.0)
    val precision = 1e-6
    for (m <- 0 until n) {
      val indices = (0 to m).toArray
      val values = indices.map(i => a(i))
      val v2 = new BSV[Double](indices, values, n)
      val norm2 = breezeNorm(v2, 2.0)
      val squaredDist = breezeSquaredDistance(v1, v2)
      val fastSquaredDist1 = fastSquaredDistance(v1, norm1, v2, norm2, precision)
      assert((fastSquaredDist1 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      val fastSquaredDist2 = fastSquaredDistance(v1, norm1, v2.toDenseVector, norm2, precision)
      assert((fastSquaredDist2 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
    }
  }

  test("compute stats") {
    val data = Seq.fill(3)(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 3.0)),
      LabeledPoint(0.0, Vectors.dense(3.0, 4.0, 5.0))
    )).flatten
    val rdd = sc.parallelize(data, 2)
    val (meanLabel, mean, std) = MLUtils.computeStats(rdd, 3, 6)
    assert(meanLabel === 0.5)
    assert(mean === Vectors.dense(2.0, 3.0, 4.0))
    assert(std === Vectors.dense(1.0, 1.0, 1.0))
  }

  test("loadLibSVMData") {
    val lines =
      """
        |+1 1:1.0 3:2.0 5:3.0
        |-1
        |-1 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Files.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    val path = tempDir.toURI.toString

    val pointsWithNumFeatures = MLUtils.loadLibSVMData(sc, path, 6).collect()
    val pointsWithoutNumFeatures = MLUtils.loadLibSVMData(sc, path).collect()

    for (points <- Seq(pointsWithNumFeatures, pointsWithoutNumFeatures)) {
      assert(points.length === 3)
      assert(points(0).label === 1.0)
      assert(points(0).features === Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
      assert(points(1).label == 0.0)
      assert(points(1).features == Vectors.sparse(6, Seq()))
      assert(points(2).label === 0.0)
      assert(points(2).features === Vectors.sparse(6, Seq((1, 4.0), (3, 5.0), (5, 6.0))))
    }

    val multiclassPoints = MLUtils.loadLibSVMData(sc, path, MLUtils.multiclassLabelParser).collect()
    assert(multiclassPoints.length === 3)
    assert(multiclassPoints(0).label === 1.0)
    assert(multiclassPoints(1).label === -1.0)
    assert(multiclassPoints(2).label === -1.0)

    try {
      file.delete()
      tempDir.delete()
    } catch {
      case t: Throwable =>
    }
  }

  // This learner always says everything is 0
  def terribleLearner(trainingData: RDD[LabeledPoint]): RegressionModel = {
    object AlwaysZero extends RegressionModel {
      override def predict(testData: RDD[Array[Double]]): RDD[Double] = {
        testData.map(_ => 0)
      }
      override def predict(testData: Array[Double]): Double = {
        0
      }
    }
    AlwaysZero
  }

  // Always returns its input
  def exactLearner(trainingData: RDD[LabeledPoint]): RegressionModel = {
    new LinearRegressionModel(Array(1.0), 0)
  }

  test("kfoldRdd") {
    val data = sc.parallelize(1 to 100, 2)
    val collectedData = data.collect().sorted
    val twoFoldedRdd = MLUtils.kFoldRdds(data, 2, 1)
    assert(twoFoldedRdd(0)._1.collect().sorted === twoFoldedRdd(1)._2.collect().sorted)
    assert(twoFoldedRdd(0)._2.collect().sorted === twoFoldedRdd(1)._1.collect().sorted)
    for (folds <- 2 to 10) {
      for (seed <- 1 to 5) {
        val foldedRdds = MLUtils.kFoldRdds(data, folds, seed)
        assert(foldedRdds.size === folds)
        foldedRdds.map{case (test, train) =>
          val result = test.union(train).collect().sorted
          assert(test.collect().size > 0, "Non empty test data")
          assert(train.collect().size > 0, "Non empty training data")
          assert(result ===  collectedData,
            "Each training+test set combined contains all of the data")
        }
        // K fold cross validation should only have each element in the test set exactly once
        assert(foldedRdds.map(_._1).reduce((x,y) => x.union(y)).collect().sorted ===
          data.collect().sorted)
      }
    }
  }

}
