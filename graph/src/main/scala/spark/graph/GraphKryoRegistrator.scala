package spark.graph

import com.esotericsoftware.kryo.Kryo

import spark.KryoRegistrator


class GraphKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    //kryo.register(classOf[(Int, Float, Float)])
    registerClass[Int, Int, Int](kryo)

    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }

  private def registerClass[VD: Manifest, ED: Manifest, VD2: Manifest](kryo: Kryo) {
    kryo.register(classOf[Vertex[VD]])
    kryo.register(classOf[Edge[ED]])
    kryo.register(classOf[MutableTuple2[VD, VD2]])
    kryo.register(classOf[(Vid, VD2)])
  }
}
