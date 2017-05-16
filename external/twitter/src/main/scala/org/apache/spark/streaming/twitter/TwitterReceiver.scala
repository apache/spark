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
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.auth.AccessToken
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}

class TwitterReceiver(level: StorageLevel) extends Receiver[String](level)
  with StatusListener with Logging {

  var twitterStream: TwitterStream = null
  var consumerKey = "<CONSUMER KEY>"
  var consumerSecret = "<CONSUMER SECRET>"
  var accessToken = "<ACCESS TOKEN>"
  var accessTokenSecret = "<ACCESS TOKEN SECRET>"

  def initStream() = {
    twitterStream = new TwitterStreamFactory().getInstance()
    twitterStream.setOAuthConsumer(consumerKey, consumerSecret)
    twitterStream.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))
    twitterStream.addListener(this)
    twitterStream
  }

  /**
    * This method is called by the system when the receiver is started. This function
    * must initialize all resources (threads, buffers, etc.) necessary for receiving data.
    * This function must be non-blocking, so receiving the data must occur on a different
    * thread. Received data can be stored with Spark by calling `store(data)`.
    *
    * If there are errors in threads started here, then following options can be done
    * (i) `reportError(...)` can be called to report the error to the driver.
    * The receiving of data will continue uninterrupted.
    * (ii) `stop(...)` can be called to stop receiving data. This will call `onStop()` to
    * clear up all resources allocated (threads, buffers, etc.) during `onStart()`.
    * (iii) `restart(...)` can be called to restart the receiver. This will call `onStop()`
    * immediately, and then `onStart()` after a delay.
    */
  override def onStart(): Unit = {
    synchronized {
      if (twitterStream == null) {
        twitterStream = initStream()
      } else {
        logWarning("Twitter receiver being asked to start more then once with out close")
      }
    }
    twitterStream.sample()
  }

  /**
    * This method is called by the system when the receiver is stopped. All resources
    * (threads, buffers, etc.) set up in `onStart()` must be cleaned up in this method.
    */
  override def onStop(): Unit = {
    synchronized {
      if (twitterStream != null) {
        twitterStream.clearListeners()
        twitterStream.cleanUp()
        twitterStream = null
      }
    }
  }

  override def onStallWarning(stallWarning: StallWarning): Unit = {}

  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

  override def onScrubGeo(l: Long, l1: Long): Unit = {}

  override def onStatus(status: Status): Unit = {
    //val dataItem = status.getUser.getScreenName  + ":" + status.getText
    val dataItem = status.getText
    this.store(dataItem)
  }

  override def onTrackLimitationNotice(i: Int): Unit = {}

  override def onException(e: Exception): Unit = {
    e.printStackTrace
    twitterStream.clearListeners()
    twitterStream.cleanUp()
  }
}
