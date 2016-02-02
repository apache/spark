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

import twitter4j.{FilterQuery, Status}
import twitter4j.auth.Authorization

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object TwitterUtils {
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
      filters: Seq[String] = Nil,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[Status] = {
      val query = new FilterQuery
      if (filters.size > 0) {
        query.track(filters.mkString(","))
      }
    new TwitterInputDStream(ssc, twitterAuth, Some(query), storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      ssc: StreamingContext,
      filters: Seq[String],
      storageLevel: StorageLevel
      ): ReceiverInputDStream[Status] = {
    val query = new FilterQuery
    if (filters.size > 0) {
      query.track(filters.mkString(","))
    }
    new TwitterInputDStream(ssc, None, Some(query), storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param filters Set of filter strings to get only those tweets that match them
   */
  def createStream(
      ssc: StreamingContext,
      filters: Seq[String]
      ): ReceiverInputDStream[Status] = {
    val query = new FilterQuery
    if (filters.size > 0) {
      query.track(filters.mkString(","))
    }
    new TwitterInputDStream(ssc, None, Some(query), StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   *        twitter4j.oauth.accessTokenSecret
   * @param query        Query object to get only those tweets that passes this query
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      ssc: StreamingContext,
      twitterAuth: Option[Authorization],
      query: FilterQuery,
      storageLevel: StorageLevel
  ): ReceiverInputDStream[Status] = {
    new TwitterInputDStream(ssc, twitterAuth, Some(query), storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   *        twitter4j.oauth.accessTokenSecret
   * @param query        Query object to get only those tweets that passes this query
   */
  def createStream(
      ssc: StreamingContext,
      twitterAuth: Option[Authorization],
      query: FilterQuery
      ): ReceiverInputDStream[Status] = {
     createStream(ssc, twitterAuth, query, StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param query        Query object to get only those tweets that passes this query
   */
  def createStream(
      ssc: StreamingContext,
      query: FilterQuery
      ): ReceiverInputDStream[Status] = {
    createStream(ssc, None, query, StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param query        Query object to get only those tweets that passes this query
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      ssc: StreamingContext,
      query: FilterQuery,
      storageLevel: StorageLevel
      ): ReceiverInputDStream[Status] = {
    createStream(ssc, None, query, storageLevel)
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
    createStream(jssc.ssc, None, filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc     JavaStreamingContext object
   * @param query    Query object to get only those tweets that passes this query
   */
  def createStream(jssc: JavaStreamingContext,
       query: FilterQuery
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, query, StorageLevel.MEMORY_AND_DISK_SER_2)
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
    createStream(jssc.ssc, None, filters, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param jssc         JavaStreamingContext object
   * @param query        Query object to get only those tweets that passes this query
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      query: FilterQuery,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, query, storageLevel)
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
    createStream(jssc.ssc, Some(twitterAuth), filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc        JavaStreamingContext object
   * @param twitterAuth Twitter4J Authorization
   * @param query       Query object to get only those tweets that passes this query
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      query: FilterQuery
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), query, StorageLevel.MEMORY_AND_DISK_SER_2)
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
    createStream(jssc.ssc, Some(twitterAuth), filters, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param jssc         JavaStreamingContext object
   * @param twitterAuth  Twitter4J Authorization object
   * @param query        Query object to get only those tweets that passes this query
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      query: FilterQuery,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), query, storageLevel)
  }
}
