package spark.util

import scala.collection.mutable.Set
import scala.collection.JavaConversions
import java.util.concurrent.ConcurrentHashMap


class TimeStampedHashSet[A] extends Set[A] {
  val internalMap = new ConcurrentHashMap[A, Long]()

  def contains(key: A): Boolean = {
    internalMap.contains(key)
  }

  def iterator: Iterator[A] = {
    val jIterator = internalMap.entrySet().iterator()
    JavaConversions.asScalaIterator(jIterator).map(_.getKey)
  }

  override def + (elem: A): Set[A] = {
    val newSet = new TimeStampedHashSet[A]
    newSet ++= this
    newSet += elem
    newSet
  }

  override def - (elem: A): Set[A] = {
    val newSet = new TimeStampedHashSet[A]
    newSet ++= this
    newSet -= elem
    newSet
  }

  override def += (key: A): this.type = {
    internalMap.put(key, currentTime)
    this
  }

  override def -= (key: A): this.type = {
    internalMap.remove(key)
    this
  }

  override def empty: Set[A] = new TimeStampedHashSet[A]()

  override def size(): Int = internalMap.size()

  override def foreach[U](f: (A) => U): Unit = {
    val iterator = internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      f(iterator.next.getKey)
    }
  }

  /**
   * Removes old values that have timestamp earlier than `threshTime`
   */
  def clearOldValues(threshTime: Long) {
    val iterator = internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue < threshTime) {
        iterator.remove()
      }
    }
  }

  private def currentTime: Long = System.currentTimeMillis()
}
