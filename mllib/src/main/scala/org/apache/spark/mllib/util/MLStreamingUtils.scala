package org.apache.spark.mllib.util

import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.mllib.regression.{LabeledPointParser, LabeledPoint}

/**
 * Helper methods to load streaming data for MLLib applications.
 */
@Experimental
object MLStreamingUtils {

  /**
   * Loads streaming labeled points from a stream of text files
   * where points are in the same format as used in `RDD[LabeledPoint].saveAsTextFile`.
   *
   * @param ssc Streaming context
   * @param path Directory path in any Hadoop-supported file system URI
   * @return Labeled points stored as a DStream[LabeledPoint]
   */
  def loadLabeledPointsFromText(ssc: StreamingContext, path: String): DStream[LabeledPoint] =
    ssc.textFileStream(path).map(LabeledPointParser.parse)

}
