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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature.{ElementwiseProduct => OldElementwiseProduct}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.types._

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
    val elemScaler = new OldElementwiseProduct(OldVectors.fromML($(scalingVec)))
    val vectorSize = $(scalingVec).size

    vector: Vector => {
      require(vector.size == vectorSize,
        s"vector sizes do not match: Expected $vectorSize but found ${vector.size}")
      vector match {
        case DenseVector(values) =>
          val newValues = elemScaler.transformDense(values)
          Vectors.dense(newValues)
        case SparseVector(size, indices, values) =>
          val (newIndices, newValues) = elemScaler.transformSparse(indices, values)
          Vectors.sparse(size, newIndices, newValues)
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
    }
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[VectorUDT],
      s"Input type must be ${(new VectorUDT).catalogString} but got ${inputType.catalogString}.")
  }

  override protected def outputDataType: DataType = new VectorUDT()

  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = super.transformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), $(scalingVec).size)
    }
    outputSchema
  }

  @Since("3.0.0")
  override def toString: String = {
    s"ElementwiseProduct: uid=$uid" +
      get(scalingVec).map(v => s", vectorSize=${v.size}").getOrElse("")
  }
}

@Since("2.0.0")
object ElementwiseProduct extends DefaultParamsReadable[ElementwiseProduct] {

  @Since("2.0.0")
  override def load(path: String): ElementwiseProduct = super.load(path)
}
