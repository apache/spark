package spark

import com.google.common.collect.MapMaker

/**
 * An implementation of Cache that uses soft references.
 */
class SoftReferenceCache extends Cache {
  val map = new MapMaker().softValues().makeMap[Any, Any]()

  override def get(key: Any): Any = map.get(key)
  override def put(key: Any, value: Any) = map.put(key, value)
}
