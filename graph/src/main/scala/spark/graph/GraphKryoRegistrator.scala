package spark.graph

import com.esotericsoftware.kryo.Kryo

import spark.KryoRegistrator


class GraphKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[(Int, Float, Float)])
    registerClass[(Int, Float, Float), Float](kryo)
    registerClass[(Int, Float), Float](kryo)
    registerClass[Int, Float](kryo)
    registerClass[Float, Float](kryo)

    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }

  private def registerClass[VD: Manifest, ED: Manifest](kryo: Kryo) {
    //kryo.register(classManifest[VD].erasure)
    // kryo.register(classManifest[ED].erasure)
    kryo.register(classOf[(Vid, Vid, ED)])
    kryo.register(classOf[(Vid, ED)])
    //kryo.register(classOf[EdgeBlockRecord[ED]])
  }
}
