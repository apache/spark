package org.apache.spark.streaming.binning

import scala.reflect.ClassTag

import org.apache.spark.streaming.dstream.DStream

class BinStream[T: ClassTag](@transient ds: DStream[T], sizeInNumBatches: Int, delayInNumBatches: Int) {
  def getDStream = ds
}
