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
import org.apache.spark.annotation.Experimental

/**
 * Generic discretizer model that transform data given a list of thresholds by feature.
 * @param thresholds Thresholds defined by feature (both must be sorted)
 *  
 * Note: checking the second sorting condition can be much more time-consuming. 
 * We omit this condition.
 */

@Experimental
class DiscretizerModel (val thresholds: Array[(Int, Seq[Float])]) extends VectorTransformer {
  
  require(isSorted(thresholds.map(_._1)), "Array has to be sorted asc")
  
  protected def isSorted(array: Array[Int]): Boolean = {
    var i = 1
    while (i < array.length) {
      if (array(i) < array(i-1)) return false
      i += 1
    }
    true
  }
  
  /**
   * Discretizes values for a single example using thresholds.
   *
   * @param data Vector.
   * @return Discretized vector (with bins from 1 to n).
   */
  override def transform(data: Vector) = {
    data match {
      case SparseVector(size, indices, values) =>
        var newValues = Array.empty[Double]
        var i = 0
        var j = 0
        while(i < indices.size && j < thresholds.size){
          val ival = indices(i)
          val th = thresholds(j)
          if (ival < th._1) {
            newValues = values(i) +: newValues
            i += 1
          } else if (ival > th._1) {
            j += 1
          } else {                  
            newValues = assignDiscreteValue(values(i), th._2).toDouble +: newValues
            j += 1
            i += 1
          }
        }
        // the `index` array inside sparse vector object will not be changed
        Vectors.sparse(size, indices, newValues)
        
        case DenseVector(values) =>
          var newValues = Array.empty[Double]
          var i = 0
          var j = 0
          while(i < values.size && j < thresholds.size){
            val th = thresholds(j)
            if (i < th._1) {
              newValues = values(i) +: newValues
              i += 1
            } else if (i > th._1) {
              j += 1
            } else {                  
              newValues = assignDiscreteValue(values(i), th._2).toDouble +: newValues
              j += 1
              i += 1
            }
          }
          Vectors.dense(newValues)
    }    
  }

  /**
   * Discretizes values in a given dataset using thresholds.
   *
   * @param data RDD with continuous-valued vectors.
   * @return RDD with discretized data (bins from 1 to n).
   */
  override def transform(data: RDD[Vector]) = {
    val bc_thresholds = data.context.broadcast(thresholds)    
    data.map {
      case SparseVector(size, indices, values) =>
        var newValues = Array.empty[Double]
        var i = 0
        var j = 0
        while(i < indices.size && j < bc_thresholds.value.size){
          val ival = indices(i)
          val th = bc_thresholds.value(j)
          if (ival < th._1) {
            newValues = values(i) +: newValues
            i += 1
          } else if (ival > th._1) {
            j += 1
          } else {                  
            newValues = assignDiscreteValue(values(i), th._2).toDouble +: newValues
            j += 1
            i += 1
          }
    }
    // the `index` array inside sparse vector object will not be changed,
    // so we can re-use it to save memory.
    Vectors.sparse(size, indices, newValues)
        
      case DenseVector(values) =>
        var newValues = Array.empty[Double]
        var i = 0
        var j = 0
        while(i < values.size && j < bc_thresholds.value.size){
          val th = bc_thresholds.value(j)
          if (i < th._1) {
            newValues = values(i) +: newValues
            i += 1
          } else if (i > th._1) {
            j += 1
          } else {                  
            newValues = assignDiscreteValue(values(i), th._2).toDouble +: newValues
            j += 1
            i += 1
            }
        }
        Vectors.dense(newValues)
    }    
  }

  /**
   * Discretizes a value with a set of intervals.
   *
   * @param value Value to be discretized
   * @param thresholds Thresholds used to assign a discrete value
   */
  private def assignDiscreteValue(value: Double, thresholds: Seq[Float]) = {
    if(thresholds.isEmpty) 1 else if (value > thresholds.last) thresholds.size + 1 
      else thresholds.indexWhere{value <= _} + 1
  }

}
