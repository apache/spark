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

package org.apache.spark.sql.catalyst.util

import scala.collection.immutable
import scala.collection.mutable

object GenericArrayDataHelper {

  // SPARK-39766: Special treatment of `immutable.ArraySeq[Any]` and `mutable.ArraySeq[Any]`
  // to ensure the scenes similar to `arrayOfAnyAsSeq` in `GenericArrayDataBenchmark` have
  // the same performance when using Scala 2.12 and Scala 2.13
  def toArray(seq: scala.collection.Seq[Any]): Array[Any] = seq match {
    case mas: mutable.ArraySeq.ofRef[_] => mas.array.asInstanceOf[Array[Any]]
    case other => other.toArray
  }

  def toArray(seqOrArray: Any): Array[Any] = seqOrArray match {
    case mas: mutable.ArraySeq.ofRef[_] => mas.array.asInstanceOf[Array[Any]]
    // Specified this as`scala.collection.Seq` because seqOrArray can be
    // `mutable.ArraySeq` in Scala 2.13
    case seq: scala.collection.Seq[Any] => seq.toArray
    case array: Array[Any] => array  // array of objects, so no need to convert
    case array: Array[_] => array.toSeq.toArray[Any] // array of primitives, so box them
  }
}
