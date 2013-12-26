package org.apache.spark.util.collection

import org.apache.spark.util.SamplingSizeTracker

/** Append-only map that keeps track of its estimated size in bytes. */
class SizeTrackingAppendOnlyMap[K, V] extends AppendOnlyMap[K, V] {

  val sizeTracker = new SamplingSizeTracker(this)

  def estimateSize() = sizeTracker.estimateSize()

  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    sizeTracker.updateMade()
  }

  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    sizeTracker.updateMade()
    newValue
  }

  override protected def growTable() {
    super.growTable()
    sizeTracker.flushSamples()
  }
}
