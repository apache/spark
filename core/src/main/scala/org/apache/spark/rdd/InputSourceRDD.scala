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

package org.apache.spark.rdd

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.{Logging, TaskContext, Partition, SparkContext}
import org.apache.spark.input.{InputPartition, InputSource}


private final class InputSourcePartition(val underlying: InputPartition, override val index: Int)
  extends Partition


private[spark] class InputSourceRDD[T: ClassTag](sc: SparkContext, inputSource: InputSource[T])
  extends RDD[T](sc, Nil) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val p = split.asInstanceOf[InputSourcePartition].underlying.asInstanceOf[InputPartition]
    val reader = inputSource.createRecordReader(p, context)

    new Iterator[T] {

      // Used to check whether we are done and make sure we only call underlying.close() once.
      private[this] var finished = false

      context.addTaskCompletionListener(_ => close())

      override def hasNext: Boolean = {
        if (!finished) {
          finished = !reader.fetchNext()
          if (finished) {
            close()
          }
        }
        !finished
      }

      override def next(): T = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        reader.get()
      }

      def close() {
        try {
          if (!finished) {
            reader.close()
          }
        } catch {
          case NonFatal(e) =>
            logWarning("Exception in RecordIterator.close()", e)
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    inputSource.getPartitions().zipWithIndex.map { case (underlying, index) =>
      new InputSourcePartition(underlying, index)
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[InputSourcePartition].underlying.getPreferredLocations.toSeq
  }
}
