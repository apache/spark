/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
