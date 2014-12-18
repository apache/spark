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
package org.apache.spark.graphx.api.java

import java.lang.{Long => JLong}
import java.util.{List => JList}

import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.{EdgePartition, EdgeRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

trait JavaEdgeRDDLike [ED, VD, This <: JavaEdgeRDDLike[ED, VD, This, R],
R <: JavaRDDLike[(PartitionID, EdgePartition[ED, VD]), R]]
  extends Serializable {

  def edgeRDD: EdgeRDDImpl[ED, VD]

  def wrapRDD(edgeRDD: RDD[(PartitionID, EdgePartition[ED, VD])]) : R

  def setName() = edgeRDD.setName("JavaEdgeRDD")

  def collect(): Array[Edge[ED]] = edgeRDD.map(_.copy()).collect().asInstanceOf[Array[Edge[ED]]]

  def count() : JLong = edgeRDD.count()

  def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    edgeRDD.compute(part, context)
  }

  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): JavaEdgeRDD[ED2, VD]

  def reverse: JavaEdgeRDD[ED, VD]

  def filter
  (epred: EdgeTriplet[VD, ED] => Boolean,
   vpred: (VertexId, VD) => Boolean): JavaEdgeRDD[ED, VD]

  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): JavaEdgeRDD[ED3, VD]

  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag]
  (f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): JavaEdgeRDD[ED2, VD2]
}
