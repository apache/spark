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

package org.apache.spark.examples.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * Samples a Twitter stream with a bounding box of supplied (latitude, longitude) pairs over
 * a sliding two second window. The stream is instantiated with credentials supplied by the
 * command line arguments. Latitude and longitude values should be set on the command line as
 * <latitude>,<longitude> with the bounding box beginning in the southwest corner. For each
 * interval, will return the first ten tweets bounded by the coordinates provided.
 *
 * Run this on your local machine as
 *
 */
object TwitterGeoBounding {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [latitude,longitude ...]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val latLons:Seq[Array[Double]] = {
      val fields = args.takeRight(args.length - 4)

      if (fields.size > 0) {
        fields.map(latlon => {
          latlon.split(",").map(_.trim.toDouble)
        })
      } else {
        Nil
      }
    }

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterGeoBounding")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, latLons=latLons)
    val tweets = stream.map(status => status.getText)

    // Print first ten tweets
    tweets.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTweets in the last two seconds bounded by: %s"
              .format(latLons.map(_.mkString("(",", ",")")).mkString(" ")))
      topList.foreach{tweet => println("  %s".format(tweet))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
