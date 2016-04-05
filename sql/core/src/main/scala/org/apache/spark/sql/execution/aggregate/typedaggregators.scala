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
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines internal implementations for aggregators.
////////////////////////////////////////////////////////////////////////////////////////////////////


class TypedSum[IN, OUT : Numeric](f: IN => OUT) extends Aggregator[IN, OUT, OUT] {
  val numeric = implicitly[Numeric[OUT]]
  override def zero: OUT = numeric.zero
  override def reduce(b: OUT, a: IN): OUT = numeric.plus(b, f(a))
  override def merge(b1: OUT, b2: OUT): OUT = numeric.plus(b1, b2)
  override def finish(reduction: OUT): OUT = reduction

  // TODO(ekl) java api support once this is exposed in scala
}


class TypedSumDouble[IN](f: IN => Double) extends Aggregator[IN, Double, Double] {
  override def zero: Double = 0.0
  override def reduce(b: Double, a: IN): Double = b + f(a)
  override def merge(b1: Double, b2: Double): Double = b1 + b2
  override def finish(reduction: Double): Double = reduction

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this(x => f.call(x).asInstanceOf[Double])
  def toColumnJava(): TypedColumn[IN, java.lang.Double] = {
    toColumn(ExpressionEncoder(), ExpressionEncoder())
      .asInstanceOf[TypedColumn[IN, java.lang.Double]]
  }
}


class TypedSumLong[IN](f: IN => Long) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0L
  override def reduce(b: Long, a: IN): Long = b + f(a)
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(reduction: Long): Long = reduction

  // Java api support
  def this(f: MapFunction[IN, java.lang.Long]) = this(x => f.call(x).asInstanceOf[Long])
  def toColumnJava(): TypedColumn[IN, java.lang.Long] = {
    toColumn(ExpressionEncoder(), ExpressionEncoder())
      .asInstanceOf[TypedColumn[IN, java.lang.Long]]
  }
}


class TypedCount[IN](f: IN => Any) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0
  override def reduce(b: Long, a: IN): Long = {
    if (f(a) == null) b else b + 1
  }
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(reduction: Long): Long = reduction

  // Java api support
  def this(f: MapFunction[IN, Object]) = this(x => f.call(x))
  def toColumnJava(): TypedColumn[IN, java.lang.Long] = {
    toColumn(ExpressionEncoder(), ExpressionEncoder())
      .asInstanceOf[TypedColumn[IN, java.lang.Long]]
  }
}


class TypedAverage[IN](f: IN => Double) extends Aggregator[IN, (Double, Long), Double] {
  override def zero: (Double, Long) = (0.0, 0L)
  override def reduce(b: (Double, Long), a: IN): (Double, Long) = (f(a) + b._1, 1 + b._2)
  override def finish(reduction: (Double, Long)): Double = reduction._1 / reduction._2
  override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this(x => f.call(x).asInstanceOf[Double])
  def toColumnJava(): TypedColumn[IN, java.lang.Double] = {
    toColumn(ExpressionEncoder(), ExpressionEncoder())
      .asInstanceOf[TypedColumn[IN, java.lang.Double]]
  }
}
