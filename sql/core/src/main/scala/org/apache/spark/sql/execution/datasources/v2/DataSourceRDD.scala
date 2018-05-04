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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.DataFormat
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType

class DataSourceRDDPartition(val index: Int, val factory: DataReaderFactory)
  extends Partition with Serializable

class DataSourceRDD(
    sc: SparkContext,
    @transient private val readerFactories: Seq[DataReaderFactory],
    schema: StructType)
  extends RDD[InternalRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    readerFactories.zipWithIndex.map {
      case (readerFactory, index) => new DataSourceRDDPartition(index, readerFactory)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val factory = split.asInstanceOf[DataSourceRDDPartition].factory
    val iter: DataReaderIterator[UnsafeRow] = factory.dataFormat() match {
      case DataFormat.ROW =>
        val reader = new RowToUnsafeDataReader(
          factory.createRowDataReader(), RowEncoder.apply(schema).resolveAndBind())
        new DataReaderIterator(reader)

      case DataFormat.UNSAFE_ROW =>
        new DataReaderIterator(factory.createUnsafeRowDataReader())

      case DataFormat.COLUMNAR_BATCH =>
        new DataReaderIterator(factory.createColumnarBatchDataReader())
          // TODO: remove this type erase hack.
          .asInstanceOf[DataReaderIterator[UnsafeRow]]
    }
    context.addTaskCompletionListener(_ => iter.close())
    new InterruptibleIterator(context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition].factory.preferredLocations()
  }
}

class RowToUnsafeDataReader(rowReader: DataReader[Row], encoder: ExpressionEncoder[Row])
  extends DataReader[UnsafeRow] {

  override def next: Boolean = rowReader.next

  override def get: UnsafeRow = encoder.toRow(rowReader.get).asInstanceOf[UnsafeRow]

  override def close(): Unit = rowReader.close()
}

class DataReaderIterator[T](reader: DataReader[T]) extends Iterator[T] {
  private[this] var valuePrepared = false

  override def hasNext: Boolean = {
    if (!valuePrepared) {
      valuePrepared = reader.next()
      // no more data, close the reader.
      if (!valuePrepared) close()
    }
    valuePrepared
  }

  override def next(): T = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    valuePrepared = false
    reader.get()
  }

  def close(): Unit = reader.close()
}
