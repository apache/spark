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

package org.apache.spark.ml.ir

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors, VectorUDT}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SQLContext}

class BM25Suite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _

  @transient var k1: Double = _
  @transient var b: Double = _
  @transient var avgDl: Double = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  def scoring(
      value: Double,
      nnz: Double,
      idf: Double,
      k1: Double,
      b: Double,
      avgDl: Double): Double = {
    (value * (k1 + 1.0) / (value + k1 * (1.0 - b + b * nnz / avgDl))) * idf
  }

  def scoringDataWithBM25(
      dataSet: Array[Vector],
      model: Vector,
      k1: Double,
      b: Double,
      avgDl: Double): Array[Double] = {
    dataSet.map {
      case data: DenseVector =>
        var score = 0.0
        val nnz = data.toArray.filter(_ > 0.0).size
        data.toArray.zip(model.toArray).map { case (x, y) =>
          score += scoring(x, nnz, y, k1, b, avgDl)
        }
        score
      case data: SparseVector =>
        var score = 0.0
        val nnz = data.values.filter(_ > 0.0).size
        data.indices.zip(data.values).map { case (id, value) =>
          score += scoring(value, nnz, model(id), k1, b, avgDl) 
        }
        score
    }
  }

  test("compute BM25 with default parameter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((numOfData + 1.0) / (x + 1.0))
    })

    val df = sqlContext.createDataFrame(sc.parallelize(data, 2).map(BM25Suite.FeatureData))

    val bm25Model = new BM25()
      .setInputCol("features")
      .setOutputCol("bm25Value")
      .fit(df)

    k1 = bm25Model.getK1
    b = bm25Model.getB
    avgDl = data.foldLeft(0.0)((len, a) => {
      var l = len
      a match {
        case d: DenseVector => l += d.toArray.filter(_ > 0.0).size.toDouble
        case s: SparseVector => l += s.values.filter(_ > 0.0).size.toDouble
      }
      l
    }) / numOfData

    val expected = scoringDataWithBM25(data, idf, k1, b, avgDl)

    bm25Model.transform(df).select("bm25Value").collect().zip(expected).foreach {
      case (x: Row, y: Double) =>
        assert(x.getDouble(0) === y, "Computed BM25 score is different with expected value.")
    }
  }

  test("compute BM25 with setter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) math.log((numOfData + 1.0) / (x + 1.0)) else 0
    })

    val df = sqlContext.createDataFrame(sc.parallelize(data, 2).map(BM25Suite.FeatureData))

    k1 = 2.0
    b = 0.5
 
    val bm25Model = new BM25()
      .setInputCol("features")
      .setOutputCol("bm25Value")
      .setMinDocFreq(1)
      .setK1(k1)
      .setB(b)
      .fit(df)

    avgDl = data.foldLeft(0.0)((len, a) => {
      var l = len
      a match {
        case d: DenseVector => l += d.toArray.filter(_ > 0.0).size.toDouble
        case s: SparseVector => l += s.values.filter(_ > 0.0).size.toDouble
      }
      l
    }) / numOfData

    val expected = scoringDataWithBM25(data, idf, k1, b, avgDl)

    bm25Model.transform(df).select("bm25Value").collect().zip(expected).foreach {
      case (x: Row, y: Double) =>
        assert(x.getDouble(0) === y, "Computed BM25 score is different with expected value.")
    }
  }
}
private object BM25Suite {
  case class FeatureData(features: Vector)
}
