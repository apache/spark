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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.v2.reader.ReadTask

class DataSourceRDDPartition[T : ClassTag](val index: Int, val readTask: ReadTask[T])
  extends Partition with Serializable

class DataSourceRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val readTasks: java.util.List[ReadTask[T]])
  extends RDD[T](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    readTasks.asScala.zipWithIndex.map {
      case (readTask, index) => new DataSourceRDDPartition(index, readTask)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val reader = split.asInstanceOf[DataSourceRDDPartition[T]].readTask.createDataReader()
    context.addTaskCompletionListener(_ => reader.close())
    val iter = new Iterator[T] {
      private[this] var valuePrepared = false

      override def hasNext: Boolean = {
        if (!valuePrepared) {
          valuePrepared = reader.next()
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
    }
    new InterruptibleIterator(context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition[T]].readTask.preferredLocations()
  }
}
