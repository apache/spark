/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.mllib.discretization

import org.apache.spark.AccumulatorParam

object MapAccumulator extends AccumulatorParam[Map[String, Int]] {

  def addInPlace(map1: Map[String, Int], map2: Map[String, Int]): Map[String, Int] = {
    if (map1 isEmpty) {
      map2
    } else if (map2 isEmpty) {
      map1
    } else {
      var result = Map.empty[String, Int]
      for ((y1, x1) <- map1; (y2, x2) <- map2) {
        if (y1.trim() == y2.trim()) {
          result += ((y1, x1 + x2))
        }
      }

      (map1.keySet diff map2.keySet) foreach { y =>
        result += ((y, map1(y)))
      }

      (map2.keySet diff map1.keySet) foreach { y =>
        result += ((y, map2(y)))
      }

      result
    }
  }

  def zero(initialValue: Map[String, Int]): Map[String, Int] = {
    Map.empty[String, Int]
  }

}
