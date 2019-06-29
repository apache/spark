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

package org.apache.spark.tdigest

import org.isarnproject.sketches.TDigest
import org.isarnproject.sketches.tdmap.TDigestMap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

@SQLUserDefinedType(udt = classOf[TDigestUDT])
case class TDigestSQL(tdigest: TDigest)

class TDigestUDT extends UserDefinedType[TDigestSQL] {
  def userClass: Class[TDigestSQL] = classOf[TDigestSQL]

  override def pyUDT: String = "isarnproject.sketches.udt.tdigest.TDigestUDT"

  override def typeName: String = "tdigest"

  override def equals(obj: Any): Boolean = {
    obj match {
      case _: TDigestUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[TDigestUDT].getName.hashCode()

  private[spark] override def asNullable: TDigestUDT = this

  def sqlType: DataType = StructType(
    StructField("delta", DoubleType, false) ::
    StructField("maxDiscrete", IntegerType, false) ::
    StructField("nclusters", IntegerType, false) ::
    StructField("clustX", ArrayType(DoubleType, false), false) ::
    StructField("clustM", ArrayType(DoubleType, false), false) ::
    Nil)

  def serialize(tdsql: TDigestSQL): Any = serializeTD(tdsql.tdigest)

  def deserialize(datum: Any): TDigestSQL = TDigestSQL(deserializeTD(datum))

  def serializeTD(td: TDigest): InternalRow = {
    val TDigest(delta, maxDiscrete, nclusters, clusters) = td
    val row = new GenericInternalRow(5)
    row.setDouble(0, delta)
    row.setInt(1, maxDiscrete)
    row.setInt(2, nclusters)
    val clustX = clusters.keys.toArray
    val clustM = clusters.values.toArray
    row.update(3, UnsafeArrayData.fromPrimitiveArray(clustX))
    row.update(4, UnsafeArrayData.fromPrimitiveArray(clustM))
    row
  }

  def deserializeTD(datum: Any): TDigest = datum match {
    case row: InternalRow =>
      require(row.numFields == 5, s"expected row length 5, got ${row.numFields}")
      val delta = row.getDouble(0)
      val maxDiscrete = row.getInt(1)
      val nclusters = row.getInt(2)
      val clustX = row.getArray(3).toDoubleArray()
      val clustM = row.getArray(4).toDoubleArray()
      val clusters = clustX.zip(clustM)
        .foldLeft(TDigestMap.empty) { case (td, e) => td + e }
      TDigest(delta, maxDiscrete, nclusters, clusters)
    case u => throw new Exception(s"failed to deserialize: $u")
  }
}

case object TDigestUDT extends TDigestUDT

case class TDigestUDAF(deltaV: Double, maxDiscreteV: Int) extends
    UserDefinedAggregateFunction {

  def deterministic: Boolean = false

  def inputSchema: StructType = StructType(StructField("x", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("tdigest", TDigestUDT) :: Nil)

  def dataType: DataType = TDigestUDT

  def initialize(buf: MutableAggregationBuffer): Unit = {
    buf(0) = TDigestSQL(TDigest.empty(deltaV, maxDiscreteV))
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buf(0) = TDigestSQL(buf.getAs[TDigestSQL](0).tdigest + input.getDouble(0))
    }
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    buf1(0) = TDigestSQL(buf1.getAs[TDigestSQL](0).tdigest ++ buf2.getAs[TDigestSQL](0).tdigest)
  }

  def evaluate(buf: Row): Any = buf.getAs[TDigestSQL](0)
}

case class TDigestAggregator(deltaV: Double, maxDiscreteV: Int) extends
    Aggregator[Double, TDigestSQL, TDigestSQL] {
  def zero: TDigestSQL = TDigestSQL(TDigest.empty(deltaV, maxDiscreteV))

  def reduce(b: TDigestSQL, a: Double): TDigestSQL = TDigestSQL(b.tdigest + a)

  def merge(b1: TDigestSQL, b2: TDigestSQL): TDigestSQL = TDigestSQL(b1.tdigest ++ b2.tdigest)

  def finish(b: TDigestSQL): TDigestSQL = b

  val serde = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[TDigestSQL]()

  def bufferEncoder: Encoder[TDigestSQL] = serde

  def outputEncoder: Encoder[TDigestSQL] = serde
}

object Benchmark {
  def apply[T](blk: => T): (Double, T) = {
    val t0 = System.currentTimeMillis
    val v = blk
    val t = System.currentTimeMillis
    ((t - t0).toDouble / 1000.0, v)
  }

  def sample[T](samples: Int)(blk: => T): Array[(Double, T)] = {
    Array.fill(samples) {
      val t0 = System.currentTimeMillis
      val v = blk
      val t = System.currentTimeMillis
      ((t - t0).toDouble / 1000.0, v)
    }
  }
}
