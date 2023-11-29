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

package org.apache.spark.util.collection

import java.util.Collections

import scala.collection.immutable
import scala.jdk.CollectionConverters._

import com.google.common.collect.{Iterators => GuavaIterators, Ordering => GuavaOrdering}

import org.apache.spark.util.SparkCollectionUtils

/**
 * Utility functions for collections.
 */
private[spark] object Utils extends SparkCollectionUtils {

  /**
   * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
   * and maintains the ordering.
   */
  def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    ordering.leastOf(input.asJava, num).iterator.asScala
  }

  /**
   * Returns an iterator over the merged contents of all given input iterators,
   * traversing every element of the input iterators.
   * Equivalent entries will not be de-duplicated.
   *
   * Callers must ensure that all the input iterators are already sorted by
   * the same ordering `ord`, otherwise the result is likely to be incorrect.
   */
  def mergeOrdered[T](inputs: Iterable[IterableOnce[T]])(
    implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    GuavaIterators.mergeSorted(
      inputs.map(_.iterator.asJava).asJava, ordering).asScala
  }

  /**
   * Only returns `Some` iff ALL elements in `input` are defined. In this case, it is
   * equivalent to `Some(input.flatten)`.
   *
   * Otherwise, returns `None`.
   */
  def sequenceToOption[T](input: Seq[Option[T]]): Option[Seq[T]] =
    if (input.forall(_.isDefined)) Some(input.flatten) else None

  /**
   * Same function as `keys.zip(values).toMap`, but has perf gain.
   */
  def toMap[K, V](keys: Iterable[K], values: Iterable[V]): Map[K, V] = {
    val builder = immutable.Map.newBuilder[K, V]
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      builder += (keyIter.next(), valueIter.next()).asInstanceOf[(K, V)]
    }
    builder.result()
  }

  /**
   * Same function as `keys.zip(values).toMap.asJava`, but has perf gain.
   */
  def toJavaMap[K, V](keys: Iterable[K], values: Iterable[V]): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]()
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      map.put(keyIter.next(), valueIter.next())
    }
    Collections.unmodifiableMap(map)
  }
}
