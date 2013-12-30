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
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}

/**
 * Subclass of [[org.apache.spark.streaming.api.java.JavaStreamingContext]] that has extra
 * functions for creating Twitter input streams.
 */
class JavaStreamingContextWithTwitter(javaStreamingContext: JavaStreamingContext)
  extends JavaStreamingContext(javaStreamingContext.ssc) {

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   */
  def twitterStream(): JavaDStream[Status] = {
    ssc.twitterStream(None)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param filters Set of filter strings to get only those tweets that match them
   */
  def twitterStream(filters: Array[String]): JavaDStream[Status] = {
    ssc.twitterStream(None, filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter using Twitter4J's default
   * OAuth authentication; this requires the system properties twitter4j.oauth.consumerKey,
   * twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   * twitter4j.oauth.accessTokenSecret.
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def twitterStream(filters: Array[String], storageLevel: StorageLevel): JavaDStream[Status] = {
    ssc.twitterStream(None, filters, storageLevel)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param twitterAuth Twitter4J Authorization
   */
  def twitterStream(twitterAuth: Authorization): JavaDStream[Status] = {
    ssc.twitterStream(Some(twitterAuth))
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param twitterAuth Twitter4J Authorization
   * @param filters Set of filter strings to get only those tweets that match them
   */
  def twitterStream(
      twitterAuth: Authorization,
      filters: Array[String]
    ): JavaDStream[Status] = {
    ssc.twitterStream(Some(twitterAuth), filters)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param twitterAuth Twitter4J Authorization object
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def twitterStream(
      twitterAuth: Authorization,
      filters: Array[String],
      storageLevel: StorageLevel
    ): JavaDStream[Status] = {
    ssc.twitterStream(Some(twitterAuth), filters, storageLevel)
  }
}
