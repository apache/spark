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

package org.apache.spark.mllib.tree.impurity

import org.apache.spark.annotation.{DeveloperApi, Experimental}

/**
 * :: Experimental ::
 * Class for calculating the
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning#Gini_impurity Gini impurity]]
 * during binary classification.
 */
@Experimental
object Gini extends Impurity {

  /**
   * :: DeveloperApi ::
   * Gini coefficient calculation
   * @param c0 count of instances with label 0
   * @param c1 count of instances with label 1
   * @return Gini coefficient value
   */
  @DeveloperApi
  override def calculate(c0: Double, c1: Double): Double = {
    if (c0 == 0 || c1 == 0) {
      0
    } else {
      val total = c0 + c1
      val f0 = c0 / total
      val f1 = c1 / total
      1 - f0 * f0 - f1 * f1
    }
  }

  override def calculate(count: Double, sum: Double, sumSquares: Double): Double =
    throw new UnsupportedOperationException("Gini.calculate")
}
