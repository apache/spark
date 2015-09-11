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

import twitter4j.Status
import twitter4j.auth.Authorization
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}

object TwitterUtils {
  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   *        twitter4j.oauth.accessTokenSecret
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   * @param locations   Bounding boxes to get geotagged tweets within them. Example:
   *        Seq(BoundingBox(-180.0,-90.0,180.0,90.0)) gives any geotagged tweet. Note that if other
   *        filters (such as track) are specified, tweets matching the locations or the other
   *        filters may be returned.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      ssc: StreamingContext,
      twitterAuth: Option[Authorization],
      count: Int = 0,
      follow: Seq[Long] = Nil,
      track: Seq[String] = Nil,
      locations: Seq[BoundingBox] = Nil,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[Status] = {
    new TwitterInputDStream(ssc, twitterAuth, count, follow, track, locations, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   *        twitter4j.oauth.accessTokenSecret
   * @param filters Set of filter strings to get only those tweets that match them
   */
  def createStream(
      ssc: StreamingContext,
      twitterAuth: Option[Authorization],
      filters: Seq[String]
    ): ReceiverInputDStream[Status] = {
    createStream(ssc, twitterAuth, 0, Nil, filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   *        twitter4j.oauth.accessTokenSecret
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      ssc: StreamingContext,
      twitterAuth: Option[Authorization],
      filters: Seq[String],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[Status] = {
    createStream(ssc, twitterAuth, 0, Nil, filters, Nil, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc   JavaStreamingContext object
   */
  def createStream(jssc: JavaStreamingContext): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc    JavaStreamingContext object
   * @param filters Set of filter strings to get only those tweets that match them
   */
  def createStream(jssc: JavaStreamingContext, filters: Array[String]
      ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, 0, Nil, filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param filters      Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      filters: Array[String],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, 0, Nil, filters, Nil, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   */
  def createStream(
      jssc: JavaStreamingContext,
      count: Int
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, count)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   */
  def createStream(
      jssc: JavaStreamingContext,
      count: Int,
      follow: Array[Long]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, count, follow)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   */
  def createStream(
      jssc: JavaStreamingContext,
      count: Int,
      follow: Array[Long],
      track: Array[String]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, count, follow, track)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   * @param locations   Bounding boxes to get geotagged tweets within them. Example:
   *        Seq(BoundingBox(-180.0,-90.0,180.0,90.0)) gives any geotagged tweet. Note that if other
   *        filters (such as track) are specified, tweets matching the locations or the other
   *        filters may be returned.
   */
  def createStream(
      jssc: JavaStreamingContext,
      count: Int,
      follow: Array[Long],
      track: Array[String],
      locations: Array[BoundingBox]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, count, follow, track, locations)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   * @param locations   Bounding boxes to get geotagged tweets within them. Example:
   *        Seq(BoundingBox(-180.0,-90.0,180.0,90.0)) gives any geotagged tweet. Note that if other
   *        filters (such as track) are specified, tweets matching the locations or the other
   *        filters may be returned.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      count: Int,
      follow: Array[Long],
      track: Array[String],
      locations: Array[BoundingBox],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, count, follow, track, locations, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc        JavaStreamingContext object
   * @param twitterAuth Twitter4J Authorization
   */
  def createStream(jssc: JavaStreamingContext, twitterAuth: Authorization
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth))
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc        JavaStreamingContext object
   * @param twitterAuth Twitter4J Authorization
   * @param filters     Set of filter strings to get only those tweets that match them
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      filters: Array[String]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), 0, Nil, filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param filters      Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      filters: Array[String],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), 0, Nil, filters, Nil, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      count: Int
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), count)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      count: Int,
      follow: Array[Long]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), count, follow)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      count: Int,
      follow: Array[Long],
      track: Array[String]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), count, follow, track)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   * @param locations   Bounding boxes to get geotagged tweets within them. Example:
   *        Seq(BoundingBox(-180.0,-90.0,180.0,90.0)) gives any geotagged tweet. Note that if other
   *        filters (such as track) are specified, tweets matching the locations or the other
   *        filters may be returned.
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      count: Int,
      follow: Array[Long],
      track: Array[String],
      locations: Array[BoundingBox]
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), count, follow, track, locations)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param count       Indicates the number of previous statuses to stream before transitioning to
   *                    the live stream
   * @param follow      Specifies the users, by ID, to receive public tweets from
   * @param track       Specifies keywords to track
   * @param locations   Bounding boxes to get geotagged tweets within them. Example:
   *        Seq(BoundingBox(-180.0,-90.0,180.0,90.0)) gives any geotagged tweet. Note that if other
   *        filters (such as track) are specified, tweets matching the locations or the other
   *        filters may be returned.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      count: Int,
      follow: Array[Long],
      track: Array[String],
      locations: Array[BoundingBox],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), count, follow, track, locations, storageLevel)
  }
}
