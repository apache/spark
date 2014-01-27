package org.apache.spark.streaming.binning

import org.apache.spark.streaming.dstream.{DStream, ProratedEventDStream, BinAlignedWindowDStream, PulsatingWindowDStream}
import org.apache.spark.streaming.Time
import scala.reflect.ClassTag


class BinStreamer[T: ClassTag](@transient ds: DStream[T], getStartTime: (T) => Time, getEndTime: (T) => Time) extends Serializable {

  def prorate(binStart: Time, binEnd: Time)(x: T) = {

      val sx = getStartTime(x)
      val ex = getEndTime(x)

      if (ex == sx) {
        (x, 1.0)
      }
      else {

        // Even though binStart is not inclusive, setting s = binStart  implies limit s as x approaches binStart+
        val s = if (sx > binStart) sx else binStart

        val e = if (ex < binEnd) ex else binEnd

        (x, (e - s) / (ex - sx))
      }
  }

  def filter(binStart: Time, binEnd: Time)(x: T) = {

    // The flow is starting in the subsequent bin
    if (getStartTime(x) > binEnd) false

    // The flow ended in the prior bin
    else if (getEndTime(x) <= binStart) false

    // s approaches from binEnd+
    else if (getStartTime(x) == binEnd && getEndTime(x) > binEnd) false

    // defensive check
    else if (getStartTime(x) > getEndTime(x)) false

    else true

  }

  def numStreams(sz: Int, delay: Int) = (sz + delay - 1)/sz + 1

  def incrementalStreams(sizeInNumBatches: Int, delayInNumBatches: Int) = {

    val num = numStreams(sizeInNumBatches, delayInNumBatches)

    Array.tabulate(num)(
      delayNumBins =>
        new BinStream(
          new ProratedEventDStream[T](ds, filter, prorate, sizeInNumBatches, delayNumBins),
          sizeInNumBatches, delayNumBins)
      )
  }

  def finalStream(sizeInNumBatches: Int, delayInNumBatches: Int) = {

    val num = numStreams(sizeInNumBatches, delayInNumBatches)

    new BinStream(
      new ProratedEventDStream[T](
        new BinAlignedWindowDStream(ds, sizeInNumBatches, num - 1),
        filter, prorate, sizeInNumBatches, num - 1),
      sizeInNumBatches, num - 1
    )
  }

  def updatedStreams(sizeInNumBatches: Int, delayInNumBatches: Int) = {

    val num = numStreams(sizeInNumBatches, delayInNumBatches)

    Array.tabulate(num)(
      delayNumBins => new BinStream(
        new ProratedEventDStream(
          new PulsatingWindowDStream(ds, sizeInNumBatches, delayNumBins),
          filter, prorate, sizeInNumBatches, delayNumBins),
        sizeInNumBatches, delayNumBins)
    )
  }

}
