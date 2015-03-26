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
import org.apache.spark.ml.param.{DoubleParam, ParamMap}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{VectorUDT, Vector}
import org.apache.spark.sql.types.DataType

/**
 * :: AlphaComponent ::
 * Normalize a vector to have unit norm using the given p-norm.
 */
@AlphaComponent
class Normalizer extends UnaryTransformer[Vector, Vector, Normalizer] {

  /**
   * Normalization in L^p^ space, p = 2 by default.
   * @group param
   */
  val p = new DoubleParam(this, "p", "the p norm value", Some(2))

  /** @group getParam */
  def getP: Double = get(p)

  /** @group setParam */
  def setP(value: Double): this.type = set(p, value)

  override protected def createTransformFunc(paramMap: ParamMap): Vector => Vector = {
    val normalizer = new feature.Normalizer(paramMap(p))
    normalizer.transform
  }

  override protected def outputDataType: DataType = new VectorUDT()
}

