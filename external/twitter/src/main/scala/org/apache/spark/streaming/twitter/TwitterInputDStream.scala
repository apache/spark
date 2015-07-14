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

import twitter4j._
import twitter4j.auth.Authorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

/** Bounding box to filter tweets by location. Units are degrees. */
case class BoundingBox(west: Double, south: Double, east: Double, north: Double)

/* A stream of Twitter statuses, potentially filtered by one or more keywords and locations.
*
* @constructor create a new Twitter stream using the supplied Twitter4J authentication credentials.
* Optional sets of string filters and geographical coordinates can be used to restrict the set of
* tweets. The Twitter API is such that this may return a sampled subset of all tweets during each
* interval. If string filters and coordinates are both set then the Twitter API will return tweets
* that satisfy either condition, not necessarily both.
*
* If no Authorization object is provided, initializes OAuth authorization using the system
* properties twitter4j.oauth.consumerKey, .consumerSecret, .accessToken and .accessTokenSecret.
*/
private[streaming]
class TwitterInputDStream(
    @transient ssc_ : StreamingContext,
    twitterAuth: Option[Authorization],
    count: Int,
    follow: Seq[Long],
    track: Seq[String],
    locations: Seq[BoundingBox],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[Status](ssc_)  {

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())

  override def getReceiver(): Receiver[Status] = {
    new TwitterReceiver(authorization, count, follow, track, locations, storageLevel)
  }
}

private[streaming]
class TwitterReceiver(
    twitterAuth: Authorization,
    count: Int,
    follow: Seq[Long],
    track: Seq[String],
    locations: Seq[BoundingBox],
    storageLevel: StorageLevel
  ) extends Receiver[Status](storageLevel) with Logging {

  @volatile private var twitterStream: TwitterStream = _
  @volatile private var stopped = false

  def onStart() {
    try {
      val newTwitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
      newTwitterStream.addListener(new StatusListener {
        def onStatus(status: Status): Unit = {
          store(status)
        }
        // Unimplemented
        def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
        def onTrackLimitationNotice(i: Int) {}
        def onScrubGeo(l: Long, l1: Long) {}
        def onStallWarning(stallWarning: StallWarning) {}
        def onException(e: Exception) {
          if (!stopped) {
            restart("Error receiving tweets", e)
          }
        }
      })

      val query = new FilterQuery
      query.count(count)
      if (follow.size > 0) {
        query.follow(follow.toArray)
      }
      if (track.size > 0) {
        query.track(track.toArray)
      }
      if (locations.size > 0) {
        query.locations(locations.flatMap(box =>
          Array(Array(box.west, box.south), Array(box.east, box.north))).toArray)
      }
      newTwitterStream.filter(query)
      setTwitterStream(newTwitterStream)
      logInfo("Twitter receiver started")
      stopped = false
    } catch {
      case e: Exception => restart("Error starting Twitter stream", e)
    }
  }

  def onStop() {
    stopped = true
    setTwitterStream(null)
    logInfo("Twitter receiver stopped")
  }

  private def setTwitterStream(newTwitterStream: TwitterStream) = synchronized {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
    twitterStream = newTwitterStream
  }
}
