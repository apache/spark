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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines internal implementations for aggregators.
////////////////////////////////////////////////////////////////////////////////////////////////////


class TypedSumDouble[IN](val f: IN => Double) extends Aggregator[IN, Double, Double] {
  override def zero: Double = 0.0
  override def reduce(b: Double, a: IN): Double = b + f(a)
  override def merge(b1: Double, b2: Double): Double = b1 + b2
  override def finish(reduction: Double): Double = reduction

  override def bufferEncoder: Encoder[Double] = ExpressionEncoder[Double]()
  override def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this((x: IN) => f.call(x).asInstanceOf[Double])

  def toColumnJava: TypedColumn[IN, java.lang.Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Double]]
  }
}


class TypedSumLong[IN](val f: IN => Long) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0L
  override def reduce(b: Long, a: IN): Long = b + f(a)
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(reduction: Long): Long = reduction

  override def bufferEncoder: Encoder[Long] = ExpressionEncoder[Long]()
  override def outputEncoder: Encoder[Long] = ExpressionEncoder[Long]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Long]) = this((x: IN) => f.call(x).asInstanceOf[Long])

  def toColumnJava: TypedColumn[IN, java.lang.Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Long]]
  }
}


class TypedCount[IN](val f: IN => Any) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0
  override def reduce(b: Long, a: IN): Long = {
    if (f(a) == null) b else b + 1
  }
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(reduction: Long): Long = reduction

  override def bufferEncoder: Encoder[Long] = ExpressionEncoder[Long]()
  override def outputEncoder: Encoder[Long] = ExpressionEncoder[Long]()

  // Java api support
  def this(f: MapFunction[IN, Object]) = this((x: IN) => f.call(x).asInstanceOf[Any])
  def toColumnJava: TypedColumn[IN, java.lang.Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Long]]
  }
}


class TypedAverage[IN](val f: IN => Double) extends Aggregator[IN, (Double, Long), Double] {
  override def zero: (Double, Long) = (0.0, 0L)
  override def reduce(b: (Double, Long), a: IN): (Double, Long) = (f(a) + b._1, 1 + b._2)
  override def finish(reduction: (Double, Long)): Double = reduction._1 / reduction._2
  override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }

  override def bufferEncoder: Encoder[(Double, Long)] = ExpressionEncoder[(Double, Long)]()
  override def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this((x: IN) => f.call(x).asInstanceOf[Double])
  def toColumnJava: TypedColumn[IN, java.lang.Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Double]]
  }
}
