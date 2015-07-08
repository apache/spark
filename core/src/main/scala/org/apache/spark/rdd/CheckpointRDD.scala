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

import org.apache.spark.{Partition, SparkContext, TaskContext}

private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
 * This RDD represents a RDD checkpoint file (similar to HadoopRDD).
 */
private[spark] abstract class CheckpointRDD[T: ClassTag](@transient sc: SparkContext)
  extends RDD[T](sc, Nil) {

  // Note: override these here to work around a MiMa bug that complains
  // about `AbstractMethodProblem`s in the RDD class if these are missing
  protected override def getPartitions: Array[Partition] = ???
  override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???

  // CheckpointRDD should not be checkpointed again
  override def checkpoint(): Unit = { }
  override def doCheckpoint(): Unit = { }
}
