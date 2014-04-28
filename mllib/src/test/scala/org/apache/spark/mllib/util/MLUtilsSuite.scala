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

import scala.math
import scala.util.Random

import org.scalatest.FunSuite

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, norm => breezeNorm,
  squaredDistance => breezeSquaredDistance}
import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils._

class MLUtilsSuite extends FunSuite with LocalSparkContext {

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

    val pointsWithNumFeatures = MLUtils.loadLibSVMData(sc, path, BinaryLabelParser, 6).collect()
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

    val multiclassPoints = MLUtils.loadLibSVMData(sc, path, MulticlassLabelParser).collect()
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

  test("kFold") {
    val data = sc.parallelize(1 to 100, 2)
    val collectedData = data.collect().sorted
    val twoFoldedRdd = MLUtils.kFold(data, 2, 1)
    assert(twoFoldedRdd(0)._1.collect().sorted === twoFoldedRdd(1)._2.collect().sorted)
    assert(twoFoldedRdd(0)._2.collect().sorted === twoFoldedRdd(1)._1.collect().sorted)
    for (folds <- 2 to 10) {
      for (seed <- 1 to 5) {
        val foldedRdds = MLUtils.kFold(data, folds, seed)
        assert(foldedRdds.size === folds)
        foldedRdds.map { case (training, validation) =>
          val result = validation.union(training).collect().sorted
          val validationSize = validation.collect().size.toFloat
          assert(validationSize > 0, "empty validation data")
          val p = 1 / folds.toFloat
          // Within 3 standard deviations of the mean
          val range = 3 * math.sqrt(100 * p * (1 - p))
          val expected = 100 * p
          val lowerBound = expected - range
          val upperBound = expected + range
          assert(validationSize > lowerBound,
            s"Validation data ($validationSize) smaller than expected ($lowerBound)" )
          assert(validationSize < upperBound,
            s"Validation data ($validationSize) larger than expected ($upperBound)" )
          assert(training.collect().size > 0, "empty training data")
          assert(result ===  collectedData,
            "Each training+validation set combined should contain all of the data.")
        }
        // K fold cross validation should only have each element in the validation set exactly once
        assert(foldedRdds.map(_._2).reduce((x,y) => x.union(y)).collect().sorted ===
          data.collect().sorted)
      }
    }
  }

}
