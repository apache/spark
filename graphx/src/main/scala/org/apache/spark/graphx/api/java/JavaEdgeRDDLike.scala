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
import javax.swing.JList

import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait JavaEdgeRDDLike [ED, VD, This <: JavaEdgeRDDLike[ED, VD, This, R],
R <: JavaRDDLike[Edge[ED], R]]
  extends Serializable {

  def wrapRDD(edgeRDD: RDD[Edge[ED]]): This

  def edgeRDD: EdgeRDD[ED, VD]

  def setName() = edgeRDD.setName("JavaEdgeRDD")

  def collect(): JList[Edge[ED]] = edgeRDD.collect().toList.asInstanceOf

  def count(): Long = edgeRDD.count()

  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag]
    (f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): JavaEdgeRDD[ED2, VD2] = {
    JavaEdgeRDD.fromEdgeRDD(edgeRDD.mapEdgePartitions(f))
  }

  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): JavaEdgeRDD[ED2, VD] = {
    JavaEdgeRDD.fromEdgeRDD[ED2, VD](edgeRDD.mapValues(f))
  }

//  def filter
//    (epred: EdgeTriplet[VD, ED] => Boolean,
//    vpred: (VertexId, VD) => Boolean): JavaEdgeRDD[ED, VD] = {
//    JavaEdgeRDD.fromEdgeRDD(edgeRDD.filter(epred, vpred))
//  }
//
//  def innerJoin[ED2: ClassTag, ED3: ClassTag]
//    (other: EdgeRDD[ED2, _])
//    (f: (VertexId, VertexId, ED, ED2) => ED3): JavaEdgeRDD[ED3, VD] = {
//    JavaEdgeRDD.fromEdgeRDD(edgeRDD.innerJoin(other)(f))
//  }
}
