package org.apache.spark.graphx

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.graphx.impl._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.BoundedPriorityQueue


class GraphKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Edge[Object]])
    kryo.register(classOf[MessageToPartition[Object]])
    kryo.register(classOf[VertexBroadcastMsg[Object]])
    kryo.register(classOf[(VertexID, Object)])
    kryo.register(classOf[EdgePartition[Object]])
    kryo.register(classOf[BitSet])
    kryo.register(classOf[VertexIdToIndexMap])
    kryo.register(classOf[VertexAttributeBlock[Object]])
    kryo.register(classOf[PartitionStrategy])
    kryo.register(classOf[BoundedPriorityQueue[Object]])

    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }
}
