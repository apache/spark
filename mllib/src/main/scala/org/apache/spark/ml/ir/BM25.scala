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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * Params for [[BM25]] and [[BM25Model]].
 */
private[ir] trait BM25Base extends Params with HasInputCol with HasOutputCol {

  /**
   * The minimum of documents in which a term should appear.
   * @group param
   */
  final val minDocFreq = new IntParam(
    this, "minDocFreq", "minimum of documents in which a term should appear for filtering")
 
  /**
   * The k1 parameter.
   * @group param
   */
  final val k1 = new DoubleParam(
    this, "k1", "the k1 parameter used in BM25 model")
 
  /**
   * The b parameter.
   * @group param
   */
  final val b = new DoubleParam(
    this, "b", "the b parameter used in BM25 model")

  setDefault(minDocFreq -> 0) 
  setDefault(k1 -> 1.2)
  setDefault(b -> 0.75)

  /** @group getParam */
  def getMinDocFreq: Int = getOrDefault(minDocFreq)

  /** @group setParam */
  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)
 
  /** @group getParam */
  def getK1: Double = getOrDefault(k1)
 
  /** @group getParam */
  def getB: Double = getOrDefault(b)
 
  /** @group setParam */
  def setK1(value: Double): this.type = set(k1, value)
 
  /** @group setParam */
  def setB(value: Double): this.type = set(b, value)
 
  /**
   * Validate and transform the input schema.
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(outputCol), DoubleType)
  }
}

/**
 * :: AlphaComponent ::
 * Compute Okapi BM25 scores (BM25) for ranking given a collection of documents.
 */
@AlphaComponent
final class BM25(override val uid: String) extends Estimator[BM25Model] with BM25Base {

  def this() = this(Identifiable.randomUID("bm25"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): BM25Model = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val idf = new feature.IDF($(minDocFreq)).fit(input)
    copyValues(new BM25Model(uid, idf)).setParent(this)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[BM25]].
 */
@AlphaComponent
class BM25Model private[ml] (
    override val uid: String,
    idfModel: feature.IDFModel)
  extends Model[BM25Model] with BM25Base {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val k1 = getK1
    val b = getB
    val avgDl = idfModel.avgDl.getOrElse(0.0)
    val idf = idfModel.idf
    def scoring(value: Double, nnz: Double, idf: Double): Double = {
      (value * (k1 + 1.0) / (value + k1 * (1.0 - b + b * nnz / avgDl))) * idf
    }
    val bm25 = udf { v: Vector =>
      val n = v.size
      var score: Double = 0.0
      val k1 = getK1
      val b = getB
      val avgDl = idfModel.avgDl
      val idf = idfModel.idf
      v match {
        case SparseVector(size, indices, values) =>
          val nnz = indices.size
          val docSize = values.filter(_ > 0.0).size
          var k = 0
          while (k < nnz) {
            score += scoring(values(k), docSize, idf(indices(k)))
            k += 1
          }
          score
        case DenseVector(values) =>
          val nnz = values.filter(_ > 0.0).size
          var j = 0
          while (j < n) {
            if (values(j) > 0.0){
              score += scoring(values(j), nnz, idf(j))
            }
            j += 1
          }
          score
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
    }
    dataset.withColumn($(outputCol), bm25(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}
