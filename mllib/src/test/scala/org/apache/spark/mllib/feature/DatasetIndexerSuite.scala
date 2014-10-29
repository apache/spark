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

package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class DatasetIndexerSuite extends FunSuite with LocalSparkContext {

  test("Can fit an empty RDD") {
    val rdd = sc.parallelize(Array.empty[Vector], 2)
    val datasetIndexer = new DatasetIndexer(maxCategories = 10)
    datasetIndexer.fit(rdd)
  }

  test("If not fitted, throws error when transforming RDD or getting feature indexes") {
    val points = Seq(Array(1.0, 2.0), Array(0.0, 1.0))
    val rdd = sc.parallelize(points.map(Vectors.dense), 2)
    val datasetIndexer = new DatasetIndexer(maxCategories = 10)
    intercept[RuntimeException] {
      datasetIndexer.transform(rdd)
      println("Did not throw error when transforming before fitting.")
    }
    intercept[RuntimeException] {
      datasetIndexer.getCategoricalFeatureIndexes
      println("Did not throw error when getting feature indexes before fitting.")
    }
  }

  test("Throws error when given RDDs with different size vectors") {
    val points1 = Seq(
      Array(1.0, 2.0),
      Array(0.0, 1.0, 2.0),
      Array(-1.0, 3.0))
    val rdd1 = sc.parallelize(points1.map(Vectors.dense), 2)
    val points2a = Seq(
      Array(1.0, 2.0),
      Array(-1.0, 3.0))
    val rdd2a = sc.parallelize(points2a.map(Vectors.dense), 2)
    val points2b = Seq(
      Array(1.0),
      Array(-1.0))
    val rdd2b = sc.parallelize(points2b.map(Vectors.dense), 2)
    val rdd3 = sc.parallelize(Array.empty[Vector], 2)

    val datasetIndexer1 = new DatasetIndexer(maxCategories = 10)
    intercept[RuntimeException] {
      datasetIndexer1.fit(rdd1)
      println("Did not throw error when fitting vectors of different lengths in same RDD.")
    }
    val datasetIndexer2 = new DatasetIndexer(maxCategories = 10)
    datasetIndexer2.fit(rdd2a)
    intercept[RuntimeException] {
      datasetIndexer2.fit(rdd2b)
      println("Did not throw error when fitting vectors of different lengths in two RDDs.")
    }
    val datasetIndexer3 = new DatasetIndexer(maxCategories = 10)
    datasetIndexer3.fit(rdd3) // does nothing
    datasetIndexer3.fit(rdd2a) // should work
  }

  test("Same result with dense and sparse vectors") {

    def testDenseSparse(densePoints: Seq[Vector], sparsePoints: Seq[Vector]): Unit = {
      assert(densePoints.zip(sparsePoints).forall { case (dv, sv) => dv.toArray === sv.toArray },
        s"typo in unit test")
      val denseRDD = sc.parallelize(densePoints, 2)
      val sparseRDD = sc.parallelize(sparsePoints, 2)

      val denseDatasetIndexer = new DatasetIndexer(maxCategories = 2)
      val sparseDatasetIndexer = new DatasetIndexer(maxCategories = 2)
      denseDatasetIndexer.fit(denseRDD)
      sparseDatasetIndexer.fit(sparseRDD)
      val denseFeatureIndexes = denseDatasetIndexer.getCategoricalFeatureIndexes
      val sparseFeatureIndexes = sparseDatasetIndexer.getCategoricalFeatureIndexes
      val categoricalFeatures = denseFeatureIndexes.keys.toSet
      assert(categoricalFeatures == sparseFeatureIndexes.keys.toSet,
        "Categorical features chosen from dense vs. sparse vectors did not match.")

      assert(denseFeatureIndexes == sparseFeatureIndexes,
        "Categorical feature value indexes chosen from dense vs. sparse vectors did not match.")
    }

    testDenseSparse(DatasetIndexerSuite.densePoints1, DatasetIndexerSuite.sparsePoints1)
    testDenseSparse(DatasetIndexerSuite.densePoints2, DatasetIndexerSuite.sparsePoints2)
  }

  test("Builds correct categorical feature value index, and transform correctly") {
    def checkCategoricalFeatureIndex(
        rdd: RDD[Vector],
        maxCategories: Int,
        categoricalFeatures: Set[Int]): Unit = {
      val datasetIndexer = new DatasetIndexer(maxCategories = maxCategories)
      datasetIndexer.fit(rdd)
      val featureIndex = datasetIndexer.getCategoricalFeatureIndexes
      assert(featureIndex.keys.toSet === categoricalFeatures)
      val indexedRDD = datasetIndexer.transform(rdd)
      categoricalFeatures.foreach { catFeature =>
        val origValueSet = rdd.collect().map(_(catFeature)).toSet
        val targetValueIndexSet = Range(0, origValueSet.size).toSet
        val valueIndex = featureIndex(catFeature)
        assert(valueIndex.keys.toSet === origValueSet)
        assert(valueIndex.values.toSet === targetValueIndexSet)
        if (origValueSet.contains(0.0)) {
          assert(valueIndex(0.0) === 0) // value 0 gets index 0
        }
        // Check transformed data
        assert(indexedRDD.map(_(catFeature)).collect().toSet === targetValueIndexSet)
      }
    }

    val rdd1 = sc.parallelize(DatasetIndexerSuite.densePoints1, 2)
    checkCategoricalFeatureIndex(rdd1, maxCategories = 2, categoricalFeatures = Set(0))
    checkCategoricalFeatureIndex(rdd1, maxCategories = 3, categoricalFeatures = Set(0, 2))

    val rdd2 = sc.parallelize(DatasetIndexerSuite.densePoints2, 2)
    checkCategoricalFeatureIndex(rdd2, maxCategories = 2, categoricalFeatures = Set(1, 2))
  }

  test("Maintain sparsity for sparse vectors") {
    def checkSparsity(points: Seq[Vector], maxCategories: Int): Unit = {
      val rdd = sc.parallelize(points, 2)
      val datasetIndexer = new DatasetIndexer(maxCategories = maxCategories)
      datasetIndexer.fit(rdd)
      val indexedPoints = datasetIndexer.transform(rdd).collect()
      points.zip(indexedPoints).foreach { case (orig: SparseVector, indexed: SparseVector) =>
        assert(orig.indices.size == indexed.indices.size)
      }
    }
    checkSparsity(DatasetIndexerSuite.sparsePoints1, maxCategories = 2)
    checkSparsity(DatasetIndexerSuite.sparsePoints2, maxCategories = 2)
  }
}

object DatasetIndexerSuite {
  private val densePoints1 = Seq(
    Array(1.0, 2.0, 0.0),
    Array(0.0, 1.0, 2.0),
    Array(0.0, 0.0, -1.0),
    Array(1.0, 3.0, 2.0)).map(Vectors.dense)
  private val sparsePoints1 = Seq(
    Vectors.sparse(3, Array(0, 1), Array(1.0, 2.0)),
    Vectors.sparse(3, Array(1, 2), Array(1.0, 2.0)),
    Vectors.sparse(3, Array(2), Array(-1.0)),
    Vectors.sparse(3, Array(0, 1, 2), Array(1.0, 3.0, 2.0)))

  private val densePoints2 = Seq(
    Array(1.0, 1.0, 0.0),
    Array(0.0, 1.0, 0.0),
    Array(-1.0, 1.0, 0.0)).map(Vectors.dense)
  private val sparsePoints2 = Seq(
    Vectors.sparse(3, Array(0, 1), Array(1.0, 1.0)),
    Vectors.sparse(3, Array(1), Array(1.0)),
    Vectors.sparse(3, Array(0, 1), Array(-1.0, 1.0)))
}
