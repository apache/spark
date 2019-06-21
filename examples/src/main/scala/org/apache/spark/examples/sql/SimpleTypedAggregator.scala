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

package org.apache.spark.examples.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

// scalastyle:off println
object SimpleTypedAggregator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("common typed aggregator implementations")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.range(20).select(('id % 3).as("key"), 'id).as[(Long, Long)]
    println("input data:")
    ds.show()

    println("running typed sum:")
    ds.groupByKey(_._1).agg(new TypedSum[(Long, Long)](_._2).toColumn).show()

    println("running typed count:")
    ds.groupByKey(_._1).agg(new TypedCount[(Long, Long)](_._2).toColumn).show()

    println("running typed average:")
    ds.groupByKey(_._1).agg(new TypedAverage[(Long, Long)](_._2.toDouble).toColumn).show()

    println("running typed minimum:")
    ds.groupByKey(_._1).agg(new TypedMin[(Long, Long)](_._2.toDouble).toColumn).show()

    println("running typed maximum:")
    ds.groupByKey(_._1).agg(new TypedMax[(Long, Long)](_._2).toColumn).show()

    spark.stop()
  }
}
// scalastyle:on println

class TypedSum[IN](val f: IN => Long) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0L
  override def reduce(b: Long, a: IN): Long = b + f(a)
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(reduction: Long): Long = reduction

  override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

class TypedCount[IN](val f: IN => Any) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0
  override def reduce(b: Long, a: IN): Long = {
    if (f(a) == null) b else b + 1
  }
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(reduction: Long): Long = reduction

  override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

class TypedAverage[IN](val f: IN => Double) extends Aggregator[IN, (Double, Long), Double] {
  override def zero: (Double, Long) = (0.0, 0L)
  override def reduce(b: (Double, Long), a: IN): (Double, Long) = (f(a) + b._1, 1 + b._2)
  override def finish(reduction: (Double, Long)): Double = reduction._1 / reduction._2
  override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }

  override def bufferEncoder: Encoder[(Double, Long)] = {
    Encoders.tuple(Encoders.scalaDouble, Encoders.scalaLong)
  }
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class TypedMin[IN](val f: IN => Double) extends Aggregator[IN, MutableDouble, Option[Double]] {
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
  override def finish(reduction: MutableDouble): Option[Double] = {
    if (reduction != null) {
      Some(reduction.value)
    } else {
      None
    }
  }

  override def bufferEncoder: Encoder[MutableDouble] = Encoders.kryo[MutableDouble]
  override def outputEncoder: Encoder[Option[Double]] = Encoders.product[Option[Double]]
}

class TypedMax[IN](val f: IN => Long) extends Aggregator[IN, MutableLong, Option[Long]] {
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
  override def finish(reduction: MutableLong): Option[Long] = {
    if (reduction != null) {
      Some(reduction.value)
    } else {
      None
    }
  }

  override def bufferEncoder: Encoder[MutableLong] = Encoders.kryo[MutableLong]
  override def outputEncoder: Encoder[Option[Long]] = Encoders.product[Option[Long]]
}

class MutableLong(var value: Long) extends Serializable

class MutableDouble(var value: Double) extends Serializable
