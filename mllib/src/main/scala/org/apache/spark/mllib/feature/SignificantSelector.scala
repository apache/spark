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

import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Model to extract significant indices from vector.
 *
 * Significant indices is vector's index that has different value for different vectors.
 *
 * For example, when you use HashingTF they create big sparse vector,
 * and this code convert to smallest vector that don't include same values indices for all vectors.
 *
 * @param indices array of significant indices.
 */
@Experimental
class SignificantSelectorModel(val indices: Array[Int]) extends VectorTransformer {

  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  override def transform(vector: Vector): Vector = vector match {
    case DenseVector(vs) =>
      Vectors.dense(indices.map(vs))

    case SparseVector(s, ids, vs) =>
      var sv_idx = 0
      var new_idx = 0
      val elements = new mutable.ListBuffer[(Int, Double)]()
      
      for (idx <- indices) {
        while (sv_idx < ids.length && ids(sv_idx) < idx) {
          sv_idx += 1
        }
        if (sv_idx < ids.length && ids(sv_idx) == idx) {
          elements += ((new_idx, vs(sv_idx)))
          sv_idx += 1
        }
        new_idx += 1
      }
      
      Vectors.sparse(indices.length, elements)

    case v =>
      throw new IllegalArgumentException("Don't support vector type " + v.getClass)
  }
}

/**
 * :: Experimental ::
 * Specialized model for equivalent vectors
 */
@Experimental
class SignificantSelectorEmptyModel extends SignificantSelectorModel(Array[Int]()) {
  
  val empty_vector = Vectors.dense(Array[Double]())
  
  override def transform(vector: Vector): Vector = empty_vector
}

/**
 * :: Experimental ::
 * Create Significant selector.
 */
@Experimental
class SignificantSelector() {

  /**
   * Returns a significant vector indices selector.
   *
   * @param sources an `RDD[Vector]` containing the vectors.
   */
  def fit(sources: RDD[Vector]): SignificantSelectorModel = {
    val sources_count = sources.count()
    val significant_indices = sources.flatMap {
        case DenseVector(vs) =>
          vs.zipWithIndex
        case SparseVector(_, ids, vs) =>
          vs.zip(ids)
        case v =>
          throw new IllegalArgumentException("Don't support vector type " + v.getClass)
      }
      .map(e => (e.swap, 1))
      .reduceByKey(_ + _)
      .map { case ((idx, value), count) => (idx, (value, count))}
      .groupByKey()
      .mapValues { e =>
        val values = e.groupBy(_._1)
        val sum = e.map(_._2).sum

        values.size + (if (sum == sources_count || values.contains(0.0)) 0 else 1)
      }
      .filter(_._2 > 1)
      .keys
      .collect()
      .sorted

    if (significant_indices.nonEmpty)
      new SignificantSelectorModel(significant_indices)
    else
      new SignificantSelectorEmptyModel()
  }
}
