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

package org.apache.spark.mllib.tree.configuration

import org.apache.spark.annotation.Since

/**
 * Enum to select the algorithm for the decision tree
 */
@Since("1.0.0")
object Algo extends Enumeration {
  @Since("1.0.0")
  type Algo = Value
  @Since("1.0.0")
  val Classification, Regression = Value

  private[mllib] def fromString(name: String): Algo = name match {
    case "classification" | "Classification" => Classification
    case "regression" | "Regression" => Regression
    case _ => throw new IllegalArgumentException(s"Did not recognize Algo name: $name")
  }
}
