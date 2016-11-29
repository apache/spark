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

package org.apache.spark.mllib.util

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

object TreeAggregateWithZeroGenerator {

  /**
    * Reduces the elements of this RDD in a multi-level tree pattern.
    *
    * treeAggregate wrapper that consumes a function to produce the zero element
    * instead of the zero element itself. Useful, when the zero element is heavy
    * but it's generator is 'small', e.g. Vectors.zeros(millions of elements)
    *
    * @param depth suggested depth of the tree (default: 2)
    * @see [[org.apache.spark.rdd.RDD#reduce]]
    */
  def apply[U: ClassTag, T: ClassTag](zeroGenerator: () => U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2) (
    data: RDD[T]): U = {

    val lazySeqOp: (Option[U], T) => Option[U] = (acc, entry) =>
      if (acc.isDefined) {
        Some(seqOp(acc.get, entry))
      } else {
        Some(seqOp(zeroGenerator(), entry))
      }

    val lazyCombOp: (Option[U], Option[U]) => Option[U] = (acc1, acc2) => {
      if (acc1.isDefined && acc2.isDefined) {
        Some(combOp(acc1.get, acc2.get))
      } else if (acc1.isDefined) {
        acc1
      } else if (acc2.isDefined) {
        acc2
      } else {
        Option.empty[U]
      }
    }

    val result = data.treeAggregate(Option.empty[U])(lazySeqOp, lazyCombOp, depth = depth)

    if (result.isDefined) {
      result.get
    } else {
      zeroGenerator()
    }
  }
}
