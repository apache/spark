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

package org.apache.spark.streaming.dstream

import org.apache.spark._
import org.apache.spark.streaming._
import storage.StorageLevel
import twitter4j._
import twitter4j.auth.Authorization
import java.util.prefs.Preferences
import twitter4j.conf.ConfigurationBuilder
import twitter4j.conf.PropertyConfiguration
import twitter4j.auth.OAuthAuthorization
import twitter4j.auth.AccessToken

/* A stream of Twitter statuses, potentially filtered by one or more keywords.
*
* @constructor create a new Twitter stream using the supplied Twitter4J authentication credentials.
* An optional set of string filters can be used to restrict the set of tweets. The Twitter API is
* such that this may return a sampled subset of all tweets during each interval.
* 
* If no Authorization object is provided, initializes OAuth authorization using the system
* properties twitter4j.oauth.consumerKey, .consumerSecret, .accessToken and .accessTokenSecret.
*/
private[streaming]
class TwitterInputDStream(
    @transient ssc_ : StreamingContext,
    twitterAuth: Option[Authorization],
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[Status](ssc_)  {
  
  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())
  
  override def getReceiver(): NetworkReceiver[Status] = {
    new TwitterReceiver(authorization, filters, storageLevel)
  }
}

private[streaming]
class TwitterReceiver(
    twitterAuth: Authorization,
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkReceiver[Status] {

  var twitterStream: TwitterStream = _
  lazy val blockGenerator = new BlockGenerator(storageLevel)

  protected override def onStart() {
    blockGenerator.start()
    twitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
    twitterStream.addListener(new StatusListener {
      def onStatus(status: Status) = {
        blockGenerator += status
      }
      // Unimplemented
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(i: Int) {}
      def onScrubGeo(l: Long, l1: Long) {}
      def onStallWarning(stallWarning: StallWarning) {}
      def onException(e: Exception) { stopOnError(e) }
    })

    val query: FilterQuery = new FilterQuery
    if (filters.size > 0) {
      query.track(filters.toArray)
      twitterStream.filter(query)
    } else {
      twitterStream.sample()
    }
    logInfo("Twitter receiver started")
  }

  protected override def onStop() {
    blockGenerator.stop()
    twitterStream.shutdown()
    logInfo("Twitter receiver stopped")
  }
}
