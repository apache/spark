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
package org.apache.spark.sql.connect.client

import scala.collection.JavaConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.sql.Row

/**
 * Util functions to help convert input functions between typed filter, map, flatMap,
 * mapPartitions etc. These functions cannot be defined inside the client Dataset class as it will
 * cause Dataset sync conflicts when used together with UDFs. Thus we define them outside, in the
 * client package.
 */
private[sql] object UdfUtils {

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

  def flatMapFuncToScalaFunc[T, U](f: FlatMapFunction[T, U]): T => TraversableOnce[U] = x =>
    f.call(x).asScala

  def mapPartitionsFuncToScalaFunc[T, U](
      f: MapPartitionsFunction[T, U]): Iterator[T] => Iterator[U] = x => f.call(x.asJava).asScala

  def foreachFuncToScalaFunc[T](f: ForeachFunction[T]): T => Unit = f.call

  def foreachPartitionFuncToScalaFunc[T](f: ForeachPartitionFunction[T]): Iterator[T] => Unit =
    x => f.call(x.asJava)
}
