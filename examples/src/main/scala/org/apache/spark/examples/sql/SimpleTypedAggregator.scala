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
