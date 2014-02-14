package org.apache.spark.util

import scala.collection.mutable.{ArrayBuffer, SynchronizedMap}

import java.util.{Collections, LinkedHashMap}
import java.util.Map.{Entry => JMapEntry}
import scala.reflect.ClassTag

/**
 * A map that bounds the number of key-value pairs present in it. It can be configured to
 * drop least recently inserted or used pair. It exposes a scala.collection.mutable.Map interface
 * to allow it to be a drop-in replacement of Scala HashMaps. Internally, a Java LinkedHashMap is
 * used to get insert-order or access-order behavior. Note that the LinkedHashMap is not
 * thread-safe and hence, it is wrapped in a Collections.synchronizedMap.
 * However, getting the Java HashMap's iterator and using it can still lead to
 * ConcurrentModificationExceptions. Hence, the iterator() function is overridden to copy the
 * all pairs into an ArrayBuffer and then return the iterator to the ArrayBuffer. Also,
 * the class apply the trait SynchronizedMap which ensures that all calls to the Scala Map API
 * are synchronized. This together ensures that ConcurrentModificationException is never thrown.
 * @param bound   max number of key-value pairs
 * @param useLRU  true = least recently used/accessed will be dropped when bound is reached,
 *                false = earliest inserted will be dropped
 */
private[spark] class BoundedHashMap[A, B](bound: Int, useLRU: Boolean)
  extends WrappedJavaHashMap[A, B, A, B] with SynchronizedMap[A, B] {

  protected[util] val internalJavaMap = Collections.synchronizedMap(new LinkedHashMap[A, B](
    bound / 8, (0.75).toFloat, useLRU) {
    override protected def removeEldestEntry(eldest: JMapEntry[A, B]): Boolean = {
      size() > bound
    }
  })

  protected[util] def newInstance[K1, V1](): WrappedJavaHashMap[K1, V1, _, _] = {
    new BoundedHashMap[K1, V1](bound, useLRU)
  }

  /**
   * Overriding iterator to make sure that the internal Java HashMap's iterator
   * is not concurrently modified.
   */
  override def iterator: Iterator[(A, B)] = {
    (new ArrayBuffer[(A, B)] ++= super.iterator).iterator
  }
}
