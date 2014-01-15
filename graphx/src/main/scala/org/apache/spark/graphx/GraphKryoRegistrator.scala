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

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.graphx.impl._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.BoundedPriorityQueue

/**
 * Registers GraphX classes with Kryo for improved performance.
 */
class GraphKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Edge[Object]])
    kryo.register(classOf[MessageToPartition[Object]])
    kryo.register(classOf[VertexBroadcastMsg[Object]])
    kryo.register(classOf[(VertexId, Object)])
    kryo.register(classOf[EdgePartition[Object]])
    kryo.register(classOf[BitSet])
    kryo.register(classOf[VertexIdToIndexMap])
    kryo.register(classOf[VertexAttributeBlock[Object]])
    kryo.register(classOf[PartitionStrategy])
    kryo.register(classOf[BoundedPriorityQueue[Object]])
    kryo.register(classOf[EdgeDirection])

    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }
}
