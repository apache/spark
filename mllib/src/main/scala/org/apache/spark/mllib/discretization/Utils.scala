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

object Utils {

  implicit class MyRichSeq[T](val seq: Seq[T]) extends AnyVal {

    def apply(indexes: Seq[Int]): Option[Seq[T]] = {
      if (indexes.length == 0) {
        None
      } else {
        Some(indexes.map(i => seq(i)))
      }
    }
  }

  def sumFreqMaps[A](map1: Map[A, Int],
      map2: Map[A, Int]) = {
    if (map1 isEmpty) {
      map2
    } else if (map2 isEmpty) {
      map1
    } else {
      Map.empty[A, Int] ++
        (for ((y1, x1) <- map1; (y2, x2) <- map2 if (y1 == y2))
          yield ((y1, x1 + x2))) ++
        (for (y <- (map1.keySet diff map2.keySet))
          yield ((y, map1(y)))) ++
        (for (y <- (map2.keySet diff map1.keySet))
          yield ((y, map2(y))))
    }
  }

  @inline def log2(x: Double) = {
    math.log(x) / math.log(2)
  }

}
