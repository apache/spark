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

package org.apache.spark.graphx.api.python

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, Partition, TaskContext}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

private[spark] class PythonVertexRDD[VD: ClassTag](
    parent: RDD[_],
    command: Array[Byte],
    envVars: Map[String, String],
    pythonIncludes: JList[String],
    preservePartitoning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends PythonRDD(
    parent,
    command,
    envVars,
    pythonIncludes,
    preservePartitoning,
    pythonExec,
    broadcastVars,
    accumulator) {

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    super.compute(split, context)
//    override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
//      firstParent[ShippableVertexPartition[VD]].iterator(part, context).next.iterator
//    }
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {

  }
}
