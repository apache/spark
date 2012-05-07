package spark

import java.io._

/**
 * Wrapper around a BoundedMemoryCache that stores serialized objects as byte arrays in order to 
 * reduce storage cost and GC overhead
 */
class SerializingCache extends Cache with Logging {
  val bmc = new BoundedMemoryCache

  override def put(datasetId: Any, partition: Int, value: Any): Boolean = {
    val ser = SparkEnv.get.serializer.newInstance()
    bmc.put(datasetId, partition, ser.serialize(value))
  }

  override def get(datasetId: Any, partition: Int): Any = {
    val bytes = bmc.get(datasetId, partition)
    if (bytes != null) {
      val ser = SparkEnv.get.serializer.newInstance()
      return ser.deserialize(bytes.asInstanceOf[Array[Byte]])
    } else {
      return null
    }
  }
}
