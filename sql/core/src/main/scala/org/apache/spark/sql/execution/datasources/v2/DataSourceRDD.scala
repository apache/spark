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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DataSourceRDDPartition(val index: Int, val inputPartition: InputPartition)
  extends Partition with Serializable

abstract class DataSourceRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition])
  extends RDD[T](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new DataSourceRDDPartition(index, inputPartition)
    }.toArray
  }

  protected def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw new SparkException(s"[BUG] Not a DataSourceRDDPartition: $split")
  }

  def createReader(split: Partition, inputPartition: InputPartition): PartitionReader[_]

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val inputPartition = castPartition(split).inputPartition
    val reader: PartitionReader[_] = createReader(split, inputPartition)

    context.addTaskCompletionListener[Unit](_ => reader.close())
    val iter = new Iterator[Any] {
      private[this] var valuePrepared = false

      override def hasNext: Boolean = {
        if (!valuePrepared) {
          valuePrepared = reader.next()
        }
        valuePrepared
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        valuePrepared = false
        reader.get()
      }
    }
    new InterruptibleIterator(context, iter.asInstanceOf[Iterator[T]])
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }
}

class DataSourceRowRDD(
  sc: SparkContext,
  @transient private val inputPartitions: Seq[InputPartition],
  partitionReaderFactory: PartitionReaderFactory)
  extends DataSourceRDD[InternalRow](sc, inputPartitions) {

  def createReader(split: Partition, inputPartition: InputPartition): PartitionReader[_] = {
      partitionReaderFactory.createReader(inputPartition)
  }
}

class DataSourceColumnarBatchRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    partitionReaderFactory: PartitionReaderFactory)
  extends DataSourceRDD[ColumnarBatch](sc, inputPartitions) {

  def createReader(split: Partition, inputPartition: InputPartition): PartitionReader[_] = {
    partitionReaderFactory.createColumnarReader(inputPartition)
  }
}
