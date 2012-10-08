package spark

import com.google.common.collect.MapMaker

/**
 * An implementation of Cache that uses soft references.
 */
private[spark] class SoftReferenceCache extends Cache {
  val map = new MapMaker().softValues().makeMap[Any, Any]()

  override def get(datasetId: Any, partition: Int): Any =
    map.get((datasetId, partition))

  override def put(datasetId: Any, partition: Int, value: Any): CachePutResponse = {
    map.put((datasetId, partition), value)
    return CachePutSuccess(0)
  }
}
