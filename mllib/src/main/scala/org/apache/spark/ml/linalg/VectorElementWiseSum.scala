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

package org.apache.spark.ml.linalg

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StructType}

/**
 * A UDAF implementing a element-wise sum on [[org.apache.spark.mllib.linalg.Vector]]
 * objects.
 *
 */
private[spark] class VectorElementWiseSum extends UserDefinedAggregateFunction  {

  override def inputSchema: StructType = new StructType().add("v", new VectorUDT())
  override def bufferSchema: StructType = new StructType().add("buff", ArrayType(DoubleType))
  // Returned Data Type
  override def dataType: DataType = new VectorUDT()
  override def deterministic: Boolean = true

  // This function is called whenever key changes
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Array.emptyDoubleArray)
  }

  // Iterate over each entry of a group
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val aggr = buffer.getAs[mutable.WrappedArray[Double]](0)
    val curr = input.getAs[Vector](0)
    if (aggr.isEmpty) {
      buffer.update(0, curr.toArray)
    } else {
      curr.foreachActive((idx, v) => {
        aggr(idx) += v
      })
      buffer.update(0, aggr)
    }
  }

  // Merge two partial aggregates
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buff1 = buffer1.getAs[mutable.WrappedArray[Double]](0)
    val buff2 = buffer2.getAs[mutable.WrappedArray[Double]](0)
    if (buff1.isEmpty) {
      buffer1.update(0, buff2)
    } else if (buff2.isEmpty) {
      buffer1.update(0, buff1)
    } else {
      for ((x, i) <- buff2.zipWithIndex) {
        buff1(i) += x
      }
      buffer1.update(0, buff1)
    }
  }

  override def evaluate(buffer: Row): Vector =
    Vectors.dense(buffer.getAs[mutable.WrappedArray[Double]](0).toArray)

}