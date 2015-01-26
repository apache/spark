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

private[spark] class HashOrdering[A] extends Ordering[A] {
  override def compare(x: A, y: A): Int = {
    val h1 = if (x == null) 0 else x.hashCode()
    val h2 = if (y == null) 0 else y.hashCode()
    if (h1 < h2) -1 else if (h1 == h2) 0 else 1
  }
}

private[spark] class NoOrdering[A] extends Ordering[A] {
  override def compare(x: A, y: A): Int = 0
}

private[spark] class KeyValueOrdering[A, B](ord1: Ordering[A], ord2: Ordering[B])
    extends Ordering[Product2[A, B]] {
  override def compare(x: Product2[A, B], y: Product2[A, B]): Int = {
    val c1 = ord1.compare(x._1, y._1)
    if (c1 != 0) c1 else ord2.compare(x._2, y._2)
  }
}
