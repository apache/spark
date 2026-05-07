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
package org.apache.spark.sql.connect

import scala.reflect.classTag

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.{Codec, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.TransformingEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.StructType

/**
 * An [[Aggregator]] that wraps a [[UserDefinedAggregateFunction]]. This allows us to execute and
 * register these (deprecated) UDAFS using the Aggregator code path.
 *
 * This implementation assumes that the aggregation buffers can be updated in place. See
 * `org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate` for more
 * information.
 */
private[connect] class UserDefinedAggregateFunctionWrapper(udaf: UserDefinedAggregateFunction)
    extends Aggregator[Row, MutableRow, Any] {
  override def zero: MutableRow = {
    val row = new MutableRow(udaf.bufferSchema)
    udaf.initialize(row)
    row
  }

  override def reduce(b: MutableRow, a: Row): MutableRow = {
    udaf.update(b, a)
    b
  }

  override def merge(b1: MutableRow, b2: MutableRow): MutableRow = {
    udaf.merge(b1, b2)
    b1
  }

  override def finish(reduction: MutableRow): Any = {
    udaf.evaluate(reduction)
  }

  override def bufferEncoder: Encoder[MutableRow] = {
    TransformingEncoder(
      classTag[MutableRow],
      RowEncoder.encoderFor(udaf.bufferSchema),
      MutableRow)
  }

  override def outputEncoder: Encoder[Any] = {
    RowEncoder
      .encoderForDataType(udaf.dataType, lenient = false)
      .asInstanceOf[Encoder[Any]]
  }
}

/**
 * Mutable row implementation that is used by [[UserDefinedAggregateFunctionWrapper]]. This code
 * assumes that it is allowed to mutate/reuse buffers during aggregation.
 */
private[connect] class MutableRow(private[this] val values: Array[Any])
    extends MutableAggregationBuffer {
  def this(schema: StructType) = this(new Array[Any](schema.length))
  def this(row: Row) = this(row.asInstanceOf[GenericRow].values)
  override def length: Int = values.length
  override def update(i: Int, value: Any): Unit = values(i) = value
  override def get(i: Int): Any = values(i)
  override def copy(): MutableRow = new MutableRow(values.clone())
  def asGenericRow: Row = new GenericRow(values)
}

private[connect] object MutableRow extends (() => Codec[MutableRow, Row]) {
  object MutableRowCodec extends Codec[MutableRow, Row] {
    override def encode(in: MutableRow): Row = in.asGenericRow
    override def decode(out: Row): MutableRow = new MutableRow(out)
  }

  override def apply(): Codec[MutableRow, Row] = MutableRowCodec
}
