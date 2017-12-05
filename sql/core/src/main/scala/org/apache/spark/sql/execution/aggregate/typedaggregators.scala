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
import org.apache.spark.sql.{Encoder, Encoders, TypedColumn}
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
  override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }
  override def finish(reduction: (Double, Long)): Double = reduction._1 / reduction._2

  override def bufferEncoder: Encoder[(Double, Long)] = ExpressionEncoder[(Double, Long)]()
  override def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Double]) = this((x: IN) => f.call(x).asInstanceOf[Double])
  def toColumnJava: TypedColumn[IN, java.lang.Double] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Double]]
  }
}

trait TypedMinDouble[IN, OUT] extends Aggregator[IN, MutableDouble, OUT] {
  val f: IN => Double
  override def zero: MutableDouble = null
  override def reduce(b: MutableDouble, a: IN): MutableDouble = {
    if (b == null) {
      new MutableDouble(f(a))
    } else {
      b.value = math.min(b.value, f(a))
      b
    }
  }
  override def merge(b1: MutableDouble, b2: MutableDouble): MutableDouble = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      b1.value = math.min(b1.value, b2.value)
      b1
    }
  }

  override def bufferEncoder: Encoder[MutableDouble] = Encoders.kryo[MutableDouble]
}

class JavaTypedMinDouble[IN](override val f: IN => Double)
  extends TypedMinDouble[IN, java.lang.Double] {
  override def outputEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()
  override def finish(reduction: MutableDouble): java.lang.Double = reduction.value
  def this(f: MapFunction[IN, java.lang.Double]) = this((x: IN) => f.call(x))
}

class ScalaTypedMinDouble[IN](override val f: IN => Double)
  extends TypedMinDouble[IN, Option[Double]] {
  override def outputEncoder: Encoder[Option[Double]] = ExpressionEncoder[Option[Double]]()
  override def finish(reduction: MutableDouble): Option[Double] = {
    if (reduction != null) {
      Some(reduction.value)
    } else {
      None
    }
  }
}

trait TypedMaxDouble[IN, OUT] extends Aggregator[IN, MutableDouble, OUT] {
  val f: IN => Double
  override def zero: MutableDouble = null
  override def reduce(b: MutableDouble, a: IN): MutableDouble = {
    if (b == null) {
      new MutableDouble(f(a))
    } else {
      b.value = math.max(b.value, f(a))
      b
    }
  }
  override def merge(b1: MutableDouble, b2: MutableDouble): MutableDouble = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      b1.value = math.max(b1.value, b2.value)
      b1
    }
  }

  override def bufferEncoder: Encoder[MutableDouble] = Encoders.kryo[MutableDouble]
}

class JavaTypedMaxDouble[IN](override val f: IN => Double)
  extends TypedMaxDouble[IN, java.lang.Double] {
  override def outputEncoder: Encoder[java.lang.Double] = ExpressionEncoder[java.lang.Double]()
  override def finish(reduction: MutableDouble): java.lang.Double = reduction.value
  def this(f: MapFunction[IN, java.lang.Double]) = this((x: IN) => f.call(x))
}

class ScalaTypedMaxDouble[IN](override val f: IN => Double)
  extends TypedMaxDouble[IN, Option[Double]] {
  override def outputEncoder: Encoder[Option[Double]] = ExpressionEncoder[Option[Double]]()
  override def finish(reduction: MutableDouble): Option[Double] = {
    if (reduction != null) {
      Some(reduction.value)
    } else {
      None
    }
  }
}

trait TypedMinLong[IN, OUT] extends Aggregator[IN, MutableLong, OUT] {
  val f: IN => Long
  override def zero: MutableLong = null
  override def reduce(b: MutableLong, a: IN): MutableLong = {
    if (b == null) {
      new MutableLong(f(a))
    } else {
      b.value = math.min(b.value, f(a))
      b
    }
  }
  override def merge(b1: MutableLong, b2: MutableLong): MutableLong = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      b1.value = math.min(b1.value, b2.value)
      b1
    }
  }

  override def bufferEncoder: Encoder[MutableLong] = Encoders.kryo[MutableLong]
}

class JavaTypedMinLong[IN](override val f: IN => Long) extends TypedMinLong[IN, java.lang.Long] {
  override def outputEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()
  override def finish(reduction: MutableLong): java.lang.Long = reduction.value
  def this(f: MapFunction[IN, java.lang.Long]) = this((x: IN) => f.call(x))
}

class ScalaTypedMinLong[IN](override val f: IN => Long) extends TypedMinLong[IN, Option[Long]] {
  override def outputEncoder: Encoder[Option[Long]] = ExpressionEncoder[Option[Long]]()
  override def finish(reduction: MutableLong): Option[Long] = {
    if (reduction != null) {
      Some(reduction.value)
    } else {
      None
    }
  }
}

trait TypedMaxLong[IN, OUT] extends Aggregator[IN, MutableLong, OUT] {
  val f: IN => Long
  override def zero: MutableLong = null
  override def reduce(b: MutableLong, a: IN): MutableLong = {
    if (b == null) {
      new MutableLong(f(a))
    } else {
      b.value = math.max(b.value, f(a))
      b
    }
  }
  override def merge(b1: MutableLong, b2: MutableLong): MutableLong = {
    if (b1 == null) {
      b2
    } else if (b2 == null) {
      b1
    } else {
      b1.value = math.max(b1.value, b2.value)
      b1
    }
  }

  override def bufferEncoder: Encoder[MutableLong] = Encoders.kryo[MutableLong]
}

class JavaTypedMaxLong[IN](override val f: IN => Long) extends TypedMaxLong[IN, java.lang.Long] {
  override def outputEncoder: Encoder[java.lang.Long] = ExpressionEncoder[java.lang.Long]()
  override def finish(reduction: MutableLong): java.lang.Long = reduction.value
  def this(f: MapFunction[IN, java.lang.Long]) = this((x: IN) => f.call(x))
}

class ScalaTypedMaxLong[IN](override val f: IN => Long) extends TypedMaxLong[IN, Option[Long]] {
  override def outputEncoder: Encoder[Option[Long]] = ExpressionEncoder[Option[Long]]()
  override def finish(reduction: MutableLong): Option[Long] = {
    if (reduction != null) {
      Some(reduction.value)
    } else {
      None
    }
  }
}

class MutableLong(var value: Long) extends Serializable

class MutableDouble(var value: Double) extends Serializable
