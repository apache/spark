package spark

import com.google.common.collect.MapMaker

/**
 * An implementation of Cache that uses soft references.
 */
class SoftReferenceCache extends Cache {
  val map = new MapMaker().softValues().makeMap[Any, Any]()

  override def get(datasetId: Any, partition: Int): Any =
    map.get((datasetId, partition))

  override def put(datasetId: Any, partition: Int, value: Any): Long = {
    map.put((datasetId, partition), value)
    return 0
  }
}
