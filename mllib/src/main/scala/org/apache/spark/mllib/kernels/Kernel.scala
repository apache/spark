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
package org.apache.spark.mllib.kernels

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Declares a trait Kernel which would serve
 * as a base trait for all classes implementing
 * Machine Learning Kernels.
 *
 **/

trait Kernel {

  /**
   * Evaluates the value of the kernel given two
   * vectorial parameters
   *
   * @param x a local Vector.
   * @param y a local Vector.
   *
   * @return the value of the Kernel function.
   *
   * */
  def evaluate(x: LabeledPoint, y: LabeledPoint): Double
}
