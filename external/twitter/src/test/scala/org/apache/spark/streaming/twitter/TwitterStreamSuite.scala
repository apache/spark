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

package org.apache.spark.streaming.twitter

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.{BeforeAndAfter, Matchers}

import scala.language.postfixOps

class TwitterStreamSuite extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {
  val conf = new SparkConf().setMaster("local[4]").setAppName("TwitterStreamSuite")
  var ssc: StreamingContext = null

  /** Run test on twitter stream */
  private def testTwitterStream(): Unit = {
    ssc = new StreamingContext(conf, Seconds(1))
    try {
      val twitterStream = TwitterUtils.createStream(ssc, StorageLevel.MEMORY_AND_DISK)
      val words = twitterStream.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      wordCounts.print()

      ssc.start()
      ssc.awaitTermination()
    } finally {
      if (ssc != null) {
        ssc.stop()
      }
    }
  }

  test("twitter input stream") {
    testTwitterStream()
  }

}
