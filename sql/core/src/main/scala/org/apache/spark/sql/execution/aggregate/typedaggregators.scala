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

class TypedSumDouble[IN](val f: IN => Double)
  extends Aggregator[IN, java.lang.Double, java.lang.Double] {

  override def zero: java.lang.Double = null
  override def reduce(b: java.lang.Double, a: IN): java.lang.Double =
    if (b == null) f(a) else b + f(a)

  override def merge(b1: java.lang.Double, b2: java.lang.Double): java.lang.Double = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      b1 + b2
    }
  }
  override def finish(reduction: java.lang.Double): java.lang.Double = reduction

  override def bufferEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()
  override def outputEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this(x => f.call(x))

  def toColumnScala: TypedColumn[IN, Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, Double]]
  }
}

class TypedSumLong[IN](val f: IN => Long)
  extends Aggregator[IN, java.lang.Long, java.lang.Long] {

  override def zero: java.lang.Long = null
  override def reduce(b: java.lang.Long, a: IN): java.lang.Long =
    if (b == null) f(a) else b + f(a)

  override def merge(b1: java.lang.Long, b2: java.lang.Long): java.lang.Long = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      b1 + b2
    }
  }
  override def finish(reduction: java.lang.Long): java.lang.Long = reduction

  override def bufferEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()
  override def outputEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Long]) = this(x => f.call(x))

  def toColumnScala: TypedColumn[IN, Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, Long]]
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
  def this(f: MapFunction[IN, Object]) = this(x => f.call(x))

  def toColumnJava: TypedColumn[IN, java.lang.Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Long]]
  }
}

class TypedAverage[IN](val f: IN => Double)
  extends Aggregator[IN, (Double, Long), java.lang.Double] {

  override def zero: (Double, Long) = (0.0, 0L)
  override def reduce(b: (Double, Long), a: IN): (Double, Long) = (f(a) + b._1, 1 + b._2)
  override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) =
    (b1._1 + b2._1, b1._2 + b2._2)
  override def finish(reduction: (Double, Long)): java.lang.Double =
    if (reduction._2 == 0) null else reduction._1 / reduction._2

  override def bufferEncoder: Encoder[(Double, Long)] = ExpressionEncoder[(Double, Long)]()
  override def outputEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this(x => f.call(x).asInstanceOf[Double])

  def toColumnScala: TypedColumn[IN, Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, Double]]
  }
}

class TypedMinDouble[IN](val f: IN => Double)
  extends Aggregator[IN, java.lang.Double, java.lang.Double] {

  override def zero: java.lang.Double = null
  override def reduce(b: java.lang.Double, a: IN): java.lang.Double =
    if (b == null) f(a) else math.min(b, f(a))

  override def merge(b1: java.lang.Double, b2: java.lang.Double): java.lang.Double = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      math.min(b1, b2)
    }
  }
  override def finish(reduction: java.lang.Double): java.lang.Double = reduction

  override def bufferEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()
  override def outputEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this(x => f.call(x))

  def toColumnScala: TypedColumn[IN, Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, Double]]
  }
}

class TypedMaxDouble[IN](val f: IN => Double)
  extends Aggregator[IN, java.lang.Double, java.lang.Double] {

  override def zero: java.lang.Double = null
  override def reduce(b: java.lang.Double, a: IN): java.lang.Double =
    if (b == null) f(a) else math.max(b, f(a))

  override def merge(b1: java.lang.Double, b2: java.lang.Double): java.lang.Double = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      math.max(b1, b2)
    }
  }
  override def finish(reduction: java.lang.Double): java.lang.Double = reduction

  override def bufferEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()
  override def outputEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this(x => f.call(x))

  def toColumnScala: TypedColumn[IN, Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, Double]]
  }
}

class TypedMinLong[IN](val f: IN => Long) extends Aggregator[IN, java.lang.Long, java.lang.Long] {

  override def zero: java.lang.Long = null
  override def reduce(b: java.lang.Long, a: IN): java.lang.Long =
    if (b == null) f(a) else math.min(b, f(a))

  override def merge(b1: java.lang.Long, b2: java.lang.Long): java.lang.Long = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      math.min(b1, b2)
    }
  }
  override def finish(reduction: java.lang.Long): java.lang.Long = reduction

  override def bufferEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()
  override def outputEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Long]) = this(x => f.call(x))

  def toColumnScala: TypedColumn[IN, Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, Long]]
  }
}

class TypedMaxLong[IN](val f: IN => Long) extends Aggregator[IN, java.lang.Long, java.lang.Long] {

  override def zero: java.lang.Long = null
  override def reduce(b: java.lang.Long, a: IN): java.lang.Long =
    if (b == null) f(a) else math.max(b, f(a))

  override def merge(b1: java.lang.Long, b2: java.lang.Long): java.lang.Long = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      math.max(b1, b2)
    }
  }
  override def finish(reduction: java.lang.Long): java.lang.Long = reduction

  override def bufferEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()
  override def outputEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Long]) = this(x => f.call(x))

  def toColumnScala: TypedColumn[IN, Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, Long]]
  }
}