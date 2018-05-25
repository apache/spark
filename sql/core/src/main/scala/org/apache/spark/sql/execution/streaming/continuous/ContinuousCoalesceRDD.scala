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

package org.apache.spark.sql.execution.streaming.continuous

import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}
import org.apache.spark.rdd.{CoalescedRDDPartition, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.continuous.shuffle.{ContinuousShuffleReadPartition, ContinuousShuffleReadRDD, ContinuousShuffleWriteRDD}

case class ContinuousCoalesceRDDPartition(index: Int) extends Partition {
  private[continuous] var writersInitialized: Boolean = false
}

class ContinuousCoalesceRDD(
    var reader: ContinuousShuffleReadRDD,
    var writer: ContinuousShuffleWriteRDD)
  extends RDD[InternalRow](writer.context, Nil) {

  override def getPartitions: Array[Partition] = Array(ContinuousCoalesceRDDPartition(0))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    assert(split.index == 0)
    // lazy initialize endpoint so writer can send to it
    reader.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint

    if (!split.asInstanceOf[ContinuousCoalesceRDDPartition].writersInitialized) {
      val threads = writer.partitions.map { part =>
        new Thread() {
          override def run(): Unit = {
            try {
              writer.compute(part, context)
            } catch {
              case e: InterruptedException => // allow clean shutdown
            }
          }
        }
      }

      context.addTaskCompletionListener { ctx =>
        threads.foreach(_.interrupt())
      }

      split.asInstanceOf[ContinuousCoalesceRDDPartition].writersInitialized = true
      threads.foreach(_.start())
    }

    reader.compute(reader.partitions(split.index), context)
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(writer) {
      def getParents(id: Int): Seq[Int] = Seq(0)
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    reader = null
    writer = null
  }
}
