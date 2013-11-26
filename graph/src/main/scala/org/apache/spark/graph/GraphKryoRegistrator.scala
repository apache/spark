package org.apache.spark.graph

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.graph.impl._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.util.collection.BitSet


class GraphKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Edge[Object]])
    kryo.register(classOf[MessageToPartition[Object]])
    kryo.register(classOf[VertexBroadcastMsg[Object]])
    kryo.register(classOf[AggregationMsg[Object]])
    kryo.register(classOf[(Vid, Object)])
    kryo.register(classOf[EdgePartition[Object]])
    kryo.register(classOf[BitSet])
    kryo.register(classOf[VertexIdToIndexMap])
    kryo.register(classOf[VertexAttributeBlock[Object]])
    kryo.register(classOf[PartitionStrategy])

    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }
}
