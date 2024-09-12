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

import scala.jdk.CollectionConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.streaming.GroupState

/**
 * Util functions to help convert input functions between typed filter, map, flatMap,
 * mapPartitions etc. This class is shared between the client and the server so that when the
 * methods are used in client UDFs, the server will be able to find them when actually executing
 * the UDFs.
 *
 * DO NOT REMOVE/CHANGE THIS OBJECT OR ANY OF ITS METHODS, THEY ARE NEEDED FOR BACKWARDS
 * COMPATIBILITY WITH OLDER CLIENTS!
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
      f: T => IterableOnce[U]): Iterator[T] => Iterator[U] = _.flatMap(f)

  def filterFuncToScalaFunc[T](f: FilterFunction[T]): T => Boolean = f.call

  def mapFunctionToScalaFunc[T, U](f: MapFunction[T, U]): T => U = f.call

  def mapPartitionsFuncToScalaFunc[T, U](
      f: MapPartitionsFunction[T, U]): Iterator[T] => Iterator[U] = x => f.call(x.asJava).asScala

  def foreachFuncToScalaFunc[T](f: ForeachFunction[T]): T => Unit = f.call

  def foreachPartitionFuncToScalaFunc[T](f: ForeachPartitionFunction[T]): Iterator[T] => Unit =
    x => f.call(x.asJava)

  def foreachBatchFuncToScalaFunc[D](f: VoidFunction2[D, java.lang.Long]): (D, Long) => Unit =
    (d, i) => f.call(d, i)

  def flatMapFuncToScalaFunc[T, U](f: FlatMapFunction[T, U]): T => IterableOnce[U] = x =>
    f.call(x).asScala

  def flatMapGroupsFuncToScalaFunc[K, V, U](
      f: FlatMapGroupsFunction[K, V, U]): (K, Iterator[V]) => IterableOnce[U] = (key, data) =>
    f.call(key, data.asJava).asScala

  def mapGroupsFuncToScalaFunc[K, V, U](f: MapGroupsFunction[K, V, U]): (K, Iterator[V]) => U =
    (key, data) => f.call(key, data.asJava)

  def coGroupFunctionToScalaFunc[K, V, U, R](
      f: CoGroupFunction[K, V, U, R]): (K, Iterator[V], Iterator[U]) => IterableOnce[R] =
    (key, left, right) => f.call(key, left.asJava, right.asJava).asScala

  def mapGroupsFuncToFlatMapAdaptor[K, V, U](
      f: (K, Iterator[V]) => U): (K, Iterator[V]) => IterableOnce[U] = {
    (key: K, it: Iterator[V]) => Iterator(f(key, it))
  }

  def mapValuesAdaptor[K, V, U, IV](
      f: (K, Iterator[V]) => IterableOnce[U],
      valueMapFunc: IV => V): (K, Iterator[IV]) => IterableOnce[U] = {
    (k: K, itr: Iterator[IV]) =>
      {
        f(k, itr.map(v => valueMapFunc(v)))
      }
  }

  def mapValuesAdaptor[K, V, U, R, IV, IU](
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R],
      valueMapFunc: IV => V,
      otherValueMapFunc: IU => U): (K, Iterator[IV], Iterator[IU]) => IterableOnce[R] = {
    (k: K, itr: Iterator[IV], otherItr: Iterator[IU]) =>
      {
        f(k, itr.map(v => valueMapFunc(v)), otherItr.map(u => otherValueMapFunc(u)))
      }
  }

  def mapValuesAdaptor[K, V, S, U, IV](
      f: (K, Iterator[V], GroupState[S]) => Iterator[U],
      valueMapFunc: IV => V): (K, Iterator[IV], GroupState[S]) => Iterator[U] = {
    (k: K, itr: Iterator[IV], s: GroupState[S]) =>
      {
        f(k, itr.map(v => valueMapFunc(v)), s)
      }
  }

  def mapGroupsWithStateFuncToFlatMapAdaptor[K, V, S, U](
      f: (K, Iterator[V], GroupState[S]) => U): (K, Iterator[V], GroupState[S]) => Iterator[U] = {
    (k: K, itr: Iterator[V], s: GroupState[S]) => Iterator(f(k, itr, s))
  }

  def mapGroupsWithStateFuncToScalaFunc[K, V, S, U](
      f: MapGroupsWithStateFunction[K, V, S, U]): (K, Iterator[V], GroupState[S]) => U = {
    (key, data, groupState) => f.call(key, data.asJava, groupState)
  }

  def flatMapGroupsWithStateFuncToScalaFunc[K, V, S, U](
      f: FlatMapGroupsWithStateFunction[K, V, S, U])
      : (K, Iterator[V], GroupState[S]) => Iterator[U] = { (key, data, groupState) =>
    f.call(key, data.asJava, groupState).asScala
  }

  def mapReduceFuncToScalaFunc[T](func: ReduceFunction[T]): (T, T) => T = func.call

  def identical[T](): T => T = t => t

  def noOp[V, K](): V => K = _ => null.asInstanceOf[K]

  def iterableOnceToSeq[A, B](f: A => IterableOnce[B]): A => Seq[B] = { value =>
    f(value).iterator.to(Seq)
  }

  // ----------------------------------------------------------------------------------------------
  // Scala Functions wrappers for java UDFs.
  // ----------------------------------------------------------------------------------------------
  //  (1 to 22).foreach { i =>
  //    val extTypeArgs = (0 to i).map(_ => "_").mkString(", ")
  //    val anyTypeArgs = (0 to i).map(_ => "Any").mkString(", ")
  //    val anyCast = s".asInstanceOf[UDF$i[$anyTypeArgs]]"
  //    val anyParams = (1 to i).map(_ => "_: Any").mkString(", ")
  //    println(s"""
  //               |def wrap(f: UDF$i[$extTypeArgs]): AnyRef = {
  //               |  f$anyCast.call($anyParams)
  //               |}""".stripMargin)
  //  }

  def wrap(f: UDF0[_]): AnyRef = { () =>
    f.asInstanceOf[UDF0[Any]].call()
  }

  def wrap(f: UDF1[_, _]): AnyRef = {
    f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
  }

  def wrap(f: UDF2[_, _, _]): AnyRef = {
    f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any)
  }

  def wrap(f: UDF3[_, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any)
  }

  def wrap(f: UDF4[_, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF5[_, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]]
      .call(_: Any, _: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF6[_, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]]
      .call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF7[_, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF8[_, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF9[_, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF10[_, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
  }

  def wrap(f: UDF11[_, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[
      UDF14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[
      UDF15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[
      UDF16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF17[
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF18[
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF19[
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF20[
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(f: UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF21[
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }

  def wrap(
      f: UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]): AnyRef = {
    f.asInstanceOf[UDF22[
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any,
      Any]]
      .call(
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any,
        _: Any)
  }
}
