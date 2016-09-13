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
import java.nio.charset.StandardCharsets

import scala.io.Source

import breeze.linalg.{squaredDistance => breezeSquaredDistance}
import com.google.common.io.Files

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, SparseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.util.TestingUtils._
import org.apache.spark.util.Utils

class MLUtilsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("epsilon computation") {
    assert(1.0 + EPSILON > 1.0, s"EPSILON is too small: $EPSILON.")
    assert(1.0 + EPSILON / 2.0 === 1.0, s"EPSILON is too big: $EPSILON.")
  }

  test("fast squared distance") {
    val a = (30 to 0 by -1).map(math.pow(2.0, _)).toArray
    val n = a.length
    val v1 = Vectors.dense(a)
    val norm1 = Vectors.norm(v1, 2.0)
    val precision = 1e-6
    for (m <- 0 until n) {
      val indices = (0 to m).toArray
      val values = indices.map(i => a(i))
      val v2 = Vectors.sparse(n, indices, values)
      val norm2 = Vectors.norm(v2, 2.0)
      val v3 = Vectors.sparse(n, indices, indices.map(i => a(i) + 0.5))
      val norm3 = Vectors.norm(v3, 2.0)
      val squaredDist = breezeSquaredDistance(v1.asBreeze, v2.asBreeze)
      val fastSquaredDist1 = fastSquaredDistance(v1, norm1, v2, norm2, precision)
      assert((fastSquaredDist1 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      val fastSquaredDist2 =
        fastSquaredDistance(v1, norm1, Vectors.dense(v2.toArray), norm2, precision)
      assert((fastSquaredDist2 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      val squaredDist2 = breezeSquaredDistance(v2.asBreeze, v3.asBreeze)
      val fastSquaredDist3 =
        fastSquaredDistance(v2, norm2, v3, norm3, precision)
      assert((fastSquaredDist3 - squaredDist2) <= precision * squaredDist2, s"failed with m = $m")
      if (m > 10) {
        val v4 = Vectors.sparse(n, indices.slice(0, m - 10),
          indices.map(i => a(i) + 0.5).slice(0, m - 10))
        val norm4 = Vectors.norm(v4, 2.0)
        val squaredDist = breezeSquaredDistance(v2.asBreeze, v4.asBreeze)
        val fastSquaredDist =
          fastSquaredDistance(v2, norm2, v4, norm4, precision)
        assert((fastSquaredDist - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      }
    }
  }

  test("loadLibSVMFile") {
    val lines =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, StandardCharsets.UTF_8)
    val path = tempDir.toURI.toString

    val pointsWithNumFeatures = loadLibSVMFile(sc, path, 6).collect()
    val pointsWithoutNumFeatures = loadLibSVMFile(sc, path).collect()

    for (points <- Seq(pointsWithNumFeatures, pointsWithoutNumFeatures)) {
      assert(points.length === 3)
      assert(points(0).label === 1.0)
      assert(points(0).features === Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
      assert(points(1).label == 0.0)
      assert(points(1).features == Vectors.sparse(6, Seq()))
      assert(points(2).label === 0.0)
      assert(points(2).features === Vectors.sparse(6, Seq((1, 4.0), (3, 5.0), (5, 6.0))))
    }

    val multiclassPoints = loadLibSVMFile(sc, path).collect()
    assert(multiclassPoints.length === 3)
    assert(multiclassPoints(0).label === 1.0)
    assert(multiclassPoints(1).label === 0.0)
    assert(multiclassPoints(2).label === 0.0)

    Utils.deleteRecursively(tempDir)
  }

  test("loadLibSVMFile throws IllegalArgumentException when indices is zero-based") {
    val lines =
      """
        |0
        |0 0:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, StandardCharsets.UTF_8)
    val path = tempDir.toURI.toString

    intercept[SparkException] {
      loadLibSVMFile(sc, path).collect()
    }
    Utils.deleteRecursively(tempDir)
  }

  test("loadLibSVMFile throws IllegalArgumentException when indices is not in ascending order") {
    val lines =
      """
        |0
        |0 3:4.0 2:5.0 6:6.0
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, StandardCharsets.UTF_8)
    val path = tempDir.toURI.toString

    intercept[SparkException] {
      loadLibSVMFile(sc, path).collect()
    }
    Utils.deleteRecursively(tempDir)
  }

  test("saveAsLibSVMFile") {
    val examples = sc.parallelize(Seq(
      LabeledPoint(1.1, Vectors.sparse(3, Seq((0, 1.23), (2, 4.56)))),
      LabeledPoint(0.0, Vectors.dense(1.01, 2.02, 3.03))
    ), 2)
    val tempDir = Utils.createTempDir()
    val outputDir = new File(tempDir, "output")
    MLUtils.saveAsLibSVMFile(examples, outputDir.toURI.toString)
    val lines = outputDir.listFiles()
      .filter(_.getName.startsWith("part-"))
      .flatMap(Source.fromFile(_).getLines())
      .toSet
    val expected = Set("1.1 1:1.23 3:4.56", "0.0 1:1.01 2:2.02 3:3.03")
    assert(lines === expected)
    Utils.deleteRecursively(tempDir)
  }

  test("appendBias") {
    val sv = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    val sv1 = appendBias(sv).asInstanceOf[SparseVector]
    assert(sv1.size === 4)
    assert(sv1.indices === Array(0, 2, 3))
    assert(sv1.values === Array(1.0, 3.0, 1.0))

    val dv = Vectors.dense(1.0, 0.0, 3.0)
    val dv1 = appendBias(dv).asInstanceOf[DenseVector]
    assert(dv1.size === 4)
    assert(dv1.values === Array(1.0, 0.0, 3.0, 1.0))
  }

  test("kFold") {
    val data = sc.parallelize(1 to 100, 2)
    val collectedData = data.collect().sorted
    val twoFoldedRdd = kFold(data, 2, 1)
    assert(twoFoldedRdd(0)._1.collect().sorted === twoFoldedRdd(1)._2.collect().sorted)
    assert(twoFoldedRdd(0)._2.collect().sorted === twoFoldedRdd(1)._1.collect().sorted)
    for (folds <- 2 to 10) {
      for (seed <- 1 to 5) {
        val foldedRdds = kFold(data, folds, seed)
        assert(foldedRdds.length === folds)
        foldedRdds.foreach { case (training, validation) =>
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
        assert(foldedRdds.map(_._2).reduce((x, y) => x.union(y)).collect().sorted ===
          data.collect().sorted)
      }
    }
  }

  test("loadVectors") {
    val vectors = sc.parallelize(Seq(
      Vectors.dense(1.0, 2.0),
      Vectors.sparse(2, Array(1), Array(-1.0)),
      Vectors.dense(0.0, 1.0)
    ), 2)
    val tempDir = Utils.createTempDir()
    val outputDir = new File(tempDir, "vectors")
    val path = outputDir.toURI.toString
    vectors.saveAsTextFile(path)
    val loaded = loadVectors(sc, path)
    assert(vectors.collect().toSet === loaded.collect().toSet)
    Utils.deleteRecursively(tempDir)
  }

  test("loadLabeledPoints") {
    val points = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0))
    ), 2)
    val tempDir = Utils.createTempDir()
    val outputDir = new File(tempDir, "points")
    val path = outputDir.toURI.toString
    points.saveAsTextFile(path)
    val loaded = loadLabeledPoints(sc, path)
    assert(points.collect().toSet === loaded.collect().toSet)
    Utils.deleteRecursively(tempDir)
  }

  test("log1pExp") {
    assert(log1pExp(76.3) ~== math.log1p(math.exp(76.3)) relTol 1E-10)
    assert(log1pExp(87296763.234) ~== 87296763.234 relTol 1E-10)

    assert(log1pExp(-13.8) ~== math.log1p(math.exp(-13.8)) absTol 1E-10)
    assert(log1pExp(-238423789.865) ~== math.log1p(math.exp(-238423789.865)) absTol 1E-10)
  }

  test("convertVectorColumnsToML") {
    val x = Vectors.sparse(2, Array(1), Array(1.0))
    val metadata = new MetadataBuilder().putLong("numFeatures", 2L).build()
    val y = Vectors.dense(2.0, 3.0)
    val z = Vectors.dense(4.0)
    val p = (5.0, z)
    val w = Vectors.dense(6.0).asML
    val df = spark.createDataFrame(Seq(
      (0, x, y, p, w)
    )).toDF("id", "x", "y", "p", "w")
      .withColumn("x", col("x"), metadata)
    val newDF1 = convertVectorColumnsToML(df)
    assert(newDF1.schema("x").metadata === metadata, "Metadata should be preserved.")
    val new1 = newDF1.first()
    assert(new1 === Row(0, x.asML, y.asML, Row(5.0, z), w))
    val new2 = convertVectorColumnsToML(df, "x", "y").first()
    assert(new2 === new1)
    val new3 = convertVectorColumnsToML(df, "y", "w").first()
    assert(new3 === Row(0, x, y.asML, Row(5.0, z), w))
    intercept[IllegalArgumentException] {
      convertVectorColumnsToML(df, "p")
    }
    intercept[IllegalArgumentException] {
      convertVectorColumnsToML(df, "p._2")
    }
  }

  test("convertVectorColumnsFromML") {
    val x = Vectors.sparse(2, Array(1), Array(1.0)).asML
    val metadata = new MetadataBuilder().putLong("numFeatures", 2L).build()
    val y = Vectors.dense(2.0, 3.0).asML
    val z = Vectors.dense(4.0).asML
    val p = (5.0, z)
    val w = Vectors.dense(6.0)
    val df = spark.createDataFrame(Seq(
      (0, x, y, p, w)
    )).toDF("id", "x", "y", "p", "w")
      .withColumn("x", col("x"), metadata)
    val newDF1 = convertVectorColumnsFromML(df)
    assert(newDF1.schema("x").metadata === metadata, "Metadata should be preserved.")
    val new1 = newDF1.first()
    assert(new1 === Row(0, Vectors.fromML(x), Vectors.fromML(y), Row(5.0, z), w))
    val new2 = convertVectorColumnsFromML(df, "x", "y").first()
    assert(new2 === new1)
    val new3 = convertVectorColumnsFromML(df, "y", "w").first()
    assert(new3 === Row(0, x, Vectors.fromML(y), Row(5.0, z), w))
    intercept[IllegalArgumentException] {
      convertVectorColumnsFromML(df, "p")
    }
    intercept[IllegalArgumentException] {
      convertVectorColumnsFromML(df, "p._2")
    }
  }

  test("convertMatrixColumnsToML") {
    val x = Matrices.sparse(3, 2, Array(0, 2, 3), Array(0, 2, 1), Array(0.0, -1.2, 0.0))
    val metadata = new MetadataBuilder().putLong("numFeatures", 2L).build()
    val y = Matrices.dense(2, 1, Array(0.2, 1.3))
    val z = Matrices.ones(1, 1)
    val p = (5.0, z)
    val w = Matrices.dense(1, 1, Array(4.5)).asML
    val df = spark.createDataFrame(Seq(
      (0, x, y, p, w)
    )).toDF("id", "x", "y", "p", "w")
      .withColumn("x", col("x"), metadata)
    val newDF1 = convertMatrixColumnsToML(df)
    assert(newDF1.schema("x").metadata === metadata, "Metadata should be preserved.")
    val new1 = newDF1.first()
    assert(new1 === Row(0, x.asML, y.asML, Row(5.0, z), w))
    val new2 = convertMatrixColumnsToML(df, "x", "y").first()
    assert(new2 === new1)
    val new3 = convertMatrixColumnsToML(df, "y", "w").first()
    assert(new3 === Row(0, x, y.asML, Row(5.0, z), w))
    intercept[IllegalArgumentException] {
      convertMatrixColumnsToML(df, "p")
    }
    intercept[IllegalArgumentException] {
      convertMatrixColumnsToML(df, "p._2")
    }
  }

  test("convertMatrixColumnsFromML") {
    val x = Matrices.sparse(3, 2, Array(0, 2, 3), Array(0, 2, 1), Array(0.0, -1.2, 0.0)).asML
    val metadata = new MetadataBuilder().putLong("numFeatures", 2L).build()
    val y = Matrices.dense(2, 1, Array(0.2, 1.3)).asML
    val z = Matrices.ones(1, 1).asML
    val p = (5.0, z)
    val w = Matrices.dense(1, 1, Array(4.5))
    val df = spark.createDataFrame(Seq(
      (0, x, y, p, w)
    )).toDF("id", "x", "y", "p", "w")
      .withColumn("x", col("x"), metadata)
    val newDF1 = convertMatrixColumnsFromML(df)
    assert(newDF1.schema("x").metadata === metadata, "Metadata should be preserved.")
    val new1 = newDF1.first()
    assert(new1 === Row(0, Matrices.fromML(x), Matrices.fromML(y), Row(5.0, z), w))
    val new2 = convertMatrixColumnsFromML(df, "x", "y").first()
    assert(new2 === new1)
    val new3 = convertMatrixColumnsFromML(df, "y", "w").first()
    assert(new3 === Row(0, x, Matrices.fromML(y), Row(5.0, z), w))
    intercept[IllegalArgumentException] {
      convertMatrixColumnsFromML(df, "p")
    }
    intercept[IllegalArgumentException] {
      convertMatrixColumnsFromML(df, "p._2")
    }
  }
}
