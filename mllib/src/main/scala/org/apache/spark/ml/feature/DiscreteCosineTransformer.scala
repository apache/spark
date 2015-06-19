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

import edu.emory.mathcs.jtransforms.dct._

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.BooleanParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

/**
 * :: Experimental ::
 * A feature transformer that takes the 1D discrete cosine transform of a real vector.
 * It returns a real vector of the same length representing the DCT. The return vector is scaled
 * such that the transform matrix is unitary.
 *
 * More information: https://en.wikipedia.org/wiki/Discrete_cosine_transform#DCT-II
 */
@Experimental
// TODO: extend to work for any numeric class
// TODO: handle 2D and 3D transforms
class DiscreteCosineTransformer(override val uid: String)
  extends UnaryTransformer[Seq[Double], Seq[Double], DiscreteCosineTransformer] {

  def this() = this(Identifiable.randomUID("dct"))

  /**
   * Indicates whether to perform the inverse DCT (true) or forward DCT (false).
   * @return
   */
  def inverse: BooleanParam = new BooleanParam(
    this, "inverse", "Set transformer to perform inverse DCT")

  /** @group setParam */
  def setInverse(value: Boolean): this.type = set(inverse, value)

  /** @group getParam */
  def getInverse: Boolean = $(inverse)

  override protected def createTransformFunc: Seq[Double] => Seq[Double] = { vec : Seq[Double] =>
    val res = vec.toArray
    val jTransformer = new DoubleDCT_1D(vec.length)
    if ($(inverse)) jTransformer.inverse(res, true) else jTransformer.forward(res, true)
    res
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(
      inputType == ArrayType(DoubleType, false),
      s"Input type must be ArrayType(DoubleType, false) but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(DoubleType, false)
}
