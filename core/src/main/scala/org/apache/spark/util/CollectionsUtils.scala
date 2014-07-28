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

package org.apache.spark.util

import java.util

import scala.reflect.{classTag, ClassTag}

private[spark] object CollectionsUtils {
  def makeBinarySearch[K : Ordering : ClassTag] : (Array[K], K) => Int = {
    // For primitive keys, we can use the natural ordering. Otherwise, use the Ordering comparator.
    classTag[K] match {
      case ClassTag.Float =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Float]], x.asInstanceOf[Float])
      case ClassTag.Double =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Double]], x.asInstanceOf[Double])
      case ClassTag.Byte =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Byte]], x.asInstanceOf[Byte])
      case ClassTag.Char =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Char]], x.asInstanceOf[Char])
      case ClassTag.Short =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Short]], x.asInstanceOf[Short])
      case ClassTag.Int =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Int]], x.asInstanceOf[Int])
      case ClassTag.Long =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Long]], x.asInstanceOf[Long])
      case _ =>
        val comparator = implicitly[Ordering[K]].asInstanceOf[java.util.Comparator[Any]]
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x, comparator)
    }
  }
}
