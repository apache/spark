package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, Duration, Interval}
import scala.reflect.ClassTag


private[streaming]
class BinAlignedWindowDStream[T: ClassTag](
                                                parent: DStream[T],
                                                sizeNumBatches: Int,
                                                delayNumBins: Int)
  extends DStream[T](parent.ssc) {

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def parentRememberDuration: Duration = rememberDuration + parent.slideDuration * sizeNumBatches * (delayNumBins + 1)

  override def compute(validTime: Time): Option[RDD[T]] = {

    val binStart = (validTime - Duration(1)).floor(slideDuration * sizeNumBatches) - slideDuration * sizeNumBatches * delayNumBins

    if ((validTime - binStart).isMultipleOf(slideDuration * sizeNumBatches)) {
      val currentWindow = new Interval(binStart + slideDuration, validTime)
      Some(new UnionRDD(ssc.sc, parent.slice(currentWindow)))
    }
    else {
      None
    }
  }
}