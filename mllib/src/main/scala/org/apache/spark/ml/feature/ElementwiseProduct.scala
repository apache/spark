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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.sql.types.DataType

/**
 * Outputs the Hadamard product (i.e., the element-wise product) of each input vector with a
 * provided "weight" vector.  In other words, it scales each column of the dataset by a scalar
 * multiplier.
 */
@Since("1.4.0")
class ElementwiseProduct @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[Vector, Vector, ElementwiseProduct] with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("elemProd"))

  /**
   * the vector to multiply with input vectors
   * @group param
   */
  @Since("2.0.0")
  val scalingVec: Param[Vector] = new Param(this, "scalingVec", "vector for hadamard product")

  /** @group setParam */
  @Since("2.0.0")
  def setScalingVec(value: Vector): this.type = set(scalingVec, value)

  /** @group getParam */
  @Since("2.0.0")
  def getScalingVec: Vector = getOrDefault(scalingVec)

  override protected def createTransformFunc: Vector => Vector = {
    require(params.contains(scalingVec), s"transformation requires a weight vector")
    val elemScaler = new feature.ElementwiseProduct($(scalingVec))
    v => elemScaler.transform(v)
  }

  override protected def outputDataType: DataType = new VectorUDT()
}

@Since("2.0.0")
object ElementwiseProduct extends DefaultParamsReadable[ElementwiseProduct] {

  @Since("2.0.0")
  override def load(path: String): ElementwiseProduct = super.load(path)
}
