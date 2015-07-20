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

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._

/**
 * Generic a discretizer model that transform data given a list of thresholds by feature.
 * 
 * @param thresholds Thresholds defined for each feature (must be sorted).
 *  
 * Note: checking the second sorting condition can be much more time-consuming. 
 * We omit this condition.
 */
class DiscretizerModel (val thresholds: Array[Array[Float]]) extends VectorTransformer {
  
  /**
   * Discretizes values in a given dataset using thresholds.
   *
   * @param data A single continuous-valued vector.
   * @return A resulting vector with its values discretized (from 1 to n).
   */
  override def transform(data: Vector) = {
    data match {
      case v: SparseVector =>
        val newValues = for (i <- 0 until v.indices.length) 
          yield assignDiscreteValue(v.values(i), thresholds(v.indices(i))).toDouble
        
        // the `index` array inside sparse vector object will not be changed,
        // so we can re-use it to save memory.
        Vectors.sparse(v.size, v.indices, newValues.toArray)
        
        case v: DenseVector =>
          val newValues = for (i <- 0 until v.values.length)
            yield assignDiscreteValue(v(i), thresholds(i)).toDouble         
          Vectors.dense(newValues.toArray)
    }    
  } 
  
  /**
   * Discretizes values in a given dataset using thresholds.
   *
   * @param data RDD with continuous-valued vectors.
   * @return RDD with discretized data (from 1 to n).
   */
  override def transform(data: RDD[Vector]) = {
    val bc_thresholds = data.context.broadcast(thresholds)    
    val result = data.map {
      case v: SparseVector =>
        val newValues = for (i <- 0 until v.indices.length) 
          yield assignDiscreteValue(v.values(i), bc_thresholds.value(v.indices(i))).toDouble
        
        // the `index` array inside sparse vector object will not be changed,
        // so we can re-use it to save memory.
        Vectors.sparse(v.size, v.indices, newValues.toArray)
        
        case v: DenseVector =>
          val newValues = for (i <- 0 until v.values.length)
            yield assignDiscreteValue(v(i), bc_thresholds.value(i)).toDouble         
          Vectors.dense(newValues.toArray)
    }  
    bc_thresholds.unpersist()
    result
  }

  /**
   * Discretizes a value with a set of intervals.
   *
   * @param value Value to be discretized.
   * @param thresholds Thresholds used to assign a discrete value
   * 
   * Note: The last threshold must be always Positive Infinity
   */
  private def assignDiscreteValue(value: Double, thresholds: Seq[Float]) = {
    if(thresholds.isEmpty) value else thresholds.indexWhere{value <= _} + 1
  }

}