package spark

import java.io._

/**
 * Wrapper around a BoundedMemoryCache that stores serialized objects as
 * byte arrays in order to reduce storage cost and GC overhead
 */
class SerializingCache extends Cache with Logging {
  val bmc = new BoundedMemoryCache

  override def put(key: Any, value: Any) {
    val ser = Serializer.newInstance()
    bmc.put(key, ser.serialize(value))
  }

  override def get(key: Any): Any = {
    val bytes = bmc.get(key)
    if (bytes != null) {
      val ser = Serializer.newInstance()
      return ser.deserialize(bytes.asInstanceOf[Array[Byte]])
    } else {
      return null
    }
  }
}
