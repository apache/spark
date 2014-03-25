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

package org.apache.spark.mllib.discretization

import org.apache.spark.AccumulatorParam

private[discretization] object MapAccumulator extends AccumulatorParam[Map[String, Int]] {

  def addInPlace(map1: Map[String, Int], map2: Map[String, Int]): Map[String, Int] = {

    if (map1 isEmpty) {
      map2
    } else if (map2 isEmpty) {
      map1
    } else {
      map2.foldLeft(map1)({ case (acc, (k,v)) => acc.updated(k, acc.getOrElse(k, 0) + 1) })
    }

  }

  def zero(initialValue: Map[String, Int]): Map[String, Int] = {
    Map.empty[String, Int]
  }

}
