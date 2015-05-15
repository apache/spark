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

package org.apache.spark.sql

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * An internal interface defining the RDD-like methods for [[DataFrame]].
 * Please use [[DataFrame]] directly, and do NOT use this.
 */
private[sql] trait RDDApi[T] {

  def cache(): this.type

  def persist(): this.type

  def persist(newLevel: StorageLevel): this.type

  def unpersist(): this.type

  def unpersist(blocking: Boolean): this.type

  def map[R: ClassTag](f: T => R): RDD[R]

  def flatMap[R: ClassTag](f: T => TraversableOnce[R]): RDD[R]

  def mapPartitions[R: ClassTag](f: Iterator[T] => Iterator[R]): RDD[R]

  def foreach(f: T => Unit): Unit

  def foreachPartition(f: Iterator[T] => Unit): Unit

  def take(n: Int): Array[T]

  def collect(): Array[T]

  def collectAsList(): java.util.List[T]

  def count(): Long

  def first(): T

  def repartition(numPartitions: Int): DataFrame

  def coalesce(numPartitions: Int): DataFrame

  def distinct: DataFrame
}
