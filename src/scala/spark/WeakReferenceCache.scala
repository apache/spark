package spark

import com.google.common.collect.MapMaker

/**
 * An implementation of Cache that uses weak references.
 */
class WeakReferenceCache extends Cache {
  val map = new MapMaker().weakValues().makeMap[Any, Any]()

  override def get(key: Any): Any = map.get(key)
  override def put(key: Any, value: Any) = map.put(key, value)
}

