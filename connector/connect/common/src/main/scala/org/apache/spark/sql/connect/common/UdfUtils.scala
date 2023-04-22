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
package org.apache.spark.sql.connect.common

import scala.collection.JavaConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.sql.Row

/**
 * Util functions to help convert input functions between typed filter, map, flatMap,
 * mapPartitions etc. This class is shared between the client and the server so that when the
 * methods are used in client UDFs, the server will be able to find them when actually executing
 * the UDFs.
 */
@SerialVersionUID(8464839273647598302L)
private[sql] object UdfUtils extends Serializable {

  def mapFuncToMapPartitionsAdaptor[T, U](f: T => U): Iterator[T] => Iterator[U] = _.map(f(_))

  def foreachFuncToForeachPartitionsAdaptor[T](f: T => Unit): Iterator[T] => Unit =
    _.foreach(f(_))

  def foreachPartitionFuncToMapPartitionsAdaptor[T](
      f: Iterator[T] => Unit): Iterator[T] => Iterator[Row] = x => {
    f(x)
    // The return constructs a minimal return iterator for mapPartitions
    Iterator.empty
  }

  def flatMapFuncToMapPartitionsAdaptor[T, U](
      f: T => TraversableOnce[U]): Iterator[T] => Iterator[U] = _.flatMap(f)

  def filterFuncToScalaFunc[T](f: FilterFunction[T]): T => Boolean = f.call

  def mapFunctionToScalaFunc[T, U](f: MapFunction[T, U]): T => U = f.call

  def mapPartitionsFuncToScalaFunc[T, U](
      f: MapPartitionsFunction[T, U]): Iterator[T] => Iterator[U] = x => f.call(x.asJava).asScala

  def foreachFuncToScalaFunc[T](f: ForeachFunction[T]): T => Unit = f.call

  def foreachPartitionFuncToScalaFunc[T](f: ForeachPartitionFunction[T]): Iterator[T] => Unit =
    x => f.call(x.asJava)

  def flatMapFuncToScalaFunc[T, U](f: FlatMapFunction[T, U]): T => TraversableOnce[U] = x =>
    f.call(x).asScala

  def flatMapGroupsFuncToScalaFunc[K, V, U](
      f: FlatMapGroupsFunction[K, V, U]): (K, Iterator[V]) => TraversableOnce[U] = (key, data) =>
    f.call(key, data.asJava).asScala

  def mapGroupsFuncToScalaFunc[K, V, U](f: MapGroupsFunction[K, V, U]): (K, Iterator[V]) => U =
    (key, data) => f.call(key, data.asJava)

  def coGroupFunctionToScalaFunc[K, V, U, R](
      f: CoGroupFunction[K, V, U, R]): (K, Iterator[V], Iterator[U]) => TraversableOnce[R] =
    (key, left, right) => f.call(key, left.asJava, right.asJava).asScala

  def mapGroupsFuncToFlatMapAdaptor[K, V, U](
      f: (K, Iterator[V]) => U): (K, Iterator[V]) => TraversableOnce[U] = {
    (key: K, it: Iterator[V]) => Iterator(f(key, it))
  }

  def mapValuesAdaptor[K, V, U, IV](
      f: (K, Iterator[V]) => TraversableOnce[U],
      valueMapFunc: IV => V): (K, Iterator[IV]) => TraversableOnce[U] = {
    (k: K, itr: Iterator[IV]) =>
      {
        f(k, itr.map(v => valueMapFunc(v)))
      }
  }

  def mapValuesAdaptor[K, V, U, R, IV, IU](
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R],
      valueMapFunc: IV => V,
      otherValueMapFunc: IU => U): (K, Iterator[IV], Iterator[IU]) => TraversableOnce[R] = {
    (k: K, itr: Iterator[IV], otherItr: Iterator[IU]) =>
      {
        f(k, itr.map(v => valueMapFunc(v)), otherItr.map(u => otherValueMapFunc(u)))
      }
  }

  def mapReduceFuncToScalaFunc[T](func: ReduceFunction[T]): (T, T) => T = func.call

  def groupAllUnderBoolTrue[T](): T => Boolean = _ => true

  def identical[T](): T => T = t => t

  def as[V, K](): V => K = v => v.asInstanceOf[K]
}
