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
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

trait JavaEdgeRDDLike [ED, This <: JavaEdgeRDDLike[ED, This, R],
R <: JavaRDDLike[(VertexId, VertexId, ED), R]]
  extends Serializable {

  def edgeRDD: EdgeRDD[ED]

  def setName() = edgeRDD.setName("JavaEdgeRDD")

  def count() : JLong = edgeRDD.count()

  def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    edgeRDD.compute(part, context)
  }

  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): JavaEdgeRDD[ED2]

  def reverse: JavaEdgeRDD[ED]
}
