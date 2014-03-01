package org.apache.spark.util.collection

import scala.collection.mutable
import java.util
import scala.collection.convert.Wrappers
import org.apache.commons.collections.map.{LRUMap => JLRUMap}

class LRUMap[K, V](maxSize: Int, underlying: util.Map[K, V]) extends Wrappers.JMapWrapper(underlying) {

  def this(maxSize: Int) = this(maxSize, new JLRUMap(maxSize).asInstanceOf[util.Map[K, V]])
}


class LRUSyncMap[K, V](maxSize: Int, underlying: util.Map[K, V])
  extends LRUMap[K, V](maxSize, underlying: util.Map[K, V]) with mutable.SynchronizedMap[K, V]