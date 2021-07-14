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

package org.apache.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.graphx.impl._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

object GraphXUtils {

  /**
   * Registers classes that GraphX uses with Kryo.
   */
  def registerKryoClasses(conf: SparkConf): Unit = {
    conf.registerKryoClasses(Array(
      classOf[Edge[Object]],
      classOf[(VertexId, Object)],
      classOf[EdgePartition[Object, Object]],
      classOf[ShippableVertexPartition[Object]],
      classOf[RoutingTablePartition],
      classOf[BitSet],
      classOf[VertexIdToIndexMap],
      classOf[VertexAttributeBlock[Object]],
      classOf[PartitionStrategy],
      classOf[BoundedPriorityQueue[Object]],
      classOf[EdgeDirection],
      classOf[GraphXPrimitiveKeyOpenHashMap[VertexId, Int]],
      classOf[OpenHashSet[Int]],
      classOf[OpenHashSet[Long]]))
  }

  /**
   * A proxy method to map the obsolete API to the new one.
   */
  private[graphx] def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
      g: Graph[VD, ED],
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None): VertexRDD[A] = {
    def sendMsg(ctx: EdgeContext[VD, ED, A]): Unit = {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }
    g.aggregateMessagesWithActiveSet(
      sendMsg, reduceFunc, TripletFields.All, activeSetOpt)
  }
}
