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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{ParamMap, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.types.DataType

/**
 * :: Experimental ::
 * Outputs the Hadamard product (i.e., the element-wise product) of each input vector with a
 * provided "weight" vector.  In other words, it scales each column of the dataset by a scalar
 * multiplier.
 */
@Experimental
class ElementwiseProduct(override val uid: String)
  extends UnaryTransformer[Vector, Vector, ElementwiseProduct] {

  def this() = this(Identifiable.randomUID("elemProd"))

  /**
    * the vector to multiply with input vectors
    * @group param
    */
  val scalingVec: Param[Vector] = new Param(this, "scalingVec", "vector for hadamard product")

  /** @group setParam */
  def setScalingVec(value: Vector): this.type = set(scalingVec, value)

  /** @group getParam */
  def getScalingVec: Vector = getOrDefault(scalingVec)

  override protected def createTransformFunc: Vector => Vector = {
    require(params.contains(scalingVec), s"transformation requires a weight vector")
    val elemScaler = new feature.ElementwiseProduct($(scalingVec))
    elemScaler.transform
  }

  override protected def outputDataType: DataType = new VectorUDT()
}
