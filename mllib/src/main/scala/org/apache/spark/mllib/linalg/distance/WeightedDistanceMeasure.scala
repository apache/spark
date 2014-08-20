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

package org.apache.spark.mllib.linalg.distance

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Abstract implementation of DistanceMeasure with support for weights.
 */
@Experimental
private[distance]
trait WeightedDistanceMeasure extends DistanceMeasure {

  val weight: Vector

  /**
   * Validates the size of the weight vector additionally
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @throws IllegalArgumentException
   */
  override def validate(v1: Vector, v2: Vector) {
    super.validate(v1, v2)

    if(!weight.size.equals(v1.size)) {
      throw new IllegalArgumentException("The size of weight vector is not equal to target vectors")
    }
  }
}
