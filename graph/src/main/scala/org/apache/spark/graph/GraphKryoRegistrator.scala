package org.apache.spark.graph

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.graph.impl.MessageToPartition
import org.apache.spark.serializer.KryoRegistrator


class GraphKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Vertex[Object]])
    kryo.register(classOf[Edge[Object]])
    kryo.register(classOf[MutableTuple2[Object, Object]])
    kryo.register(classOf[MessageToPartition[Object]])

    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }
}
