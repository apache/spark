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

import breeze.linalg.{DenseVector => DBV, Vector => BV}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

import scala.language.implicitConversions

/**
 * :: Experimental ::
 * This trait is used for objects which can determine a distance between two points
 *
 * Classes which inherits from this class are required to satisfy the follow condition:
 * 1. d(x, y) >= 0 (non-negative)
 * 2. d(x, y) = 0 if and only if x = y (identity of indiscernibles)
 * 3. d(x, y) = d(y, x) (symmetry)
 * However, classes which inherits aren't require to satisfy triangle inequality
 */
@Experimental
trait DistanceMeasure extends Function2[BV[Double], BV[Double], Double] with Serializable {

  // each measure/metric defines for itself:
  override def apply(v1: BV[Double], v2: BV[Double]): Double

  // a catch-all overloading of "()" for spark vectors
  // can also be overridden on a per-class basis, if it is advantageous
  def apply(v1: Vector, v2: Vector): Double = this(v1.toBreeze, v2.toBreeze)
}


object DistanceMeasure {

  /**
   * Implicit method for DistanceMeasure
   *
   * @param f calculating distance function (Vector, Vector) => Double
   * @return DistanceMeasure
   */
  implicit def functionToDistanceMeasure(f: (Vector, Vector) => Double): DistanceMeasure = new
      DistanceMeasure {
    override def apply(v1: Vector, v2: Vector): Double = f(v1, v2)

    override def apply(v1: BV[Double], v2: BV[Double]): Double = {
      throw new NotImplementedError(s"This DistanceMeasure is made by a lambda function")
    }
  }
}

