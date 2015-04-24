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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.types.DataType

/**
 * :: AlphaComponent
 * Maps a vector to the hadamard product of it and a reference vector.
 */
@AlphaComponent
class ElementwiseProduct extends UnaryTransformer[Vector, Vector, ElementwiseProduct] {

  /** the vector to multiply with input vectors */
  val scalingVec : Param[Vector] = new Param(this, "scalingVector", "vector for hadamard product")
  def setScalingVec(value: Vector): this.type = set(scalingVec, value)
  def getScalingVec: Vector = getOrDefault(scalingVec)

  override protected def createTransformFunc(paramMap: ParamMap): Vector => Vector = {
    val elemScaler = new feature.ElementwiseProduct(paramMap(scalingVec))
    elemScaler.transform
  }

  override protected def outputDataType: DataType = new VectorUDT()
}
