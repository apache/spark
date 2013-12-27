package org.apache.spark.streaming.twitter

import twitter4j.Status
import twitter4j.auth.Authorization
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._


class StreamingContextWithTwitter(ssc: StreamingContext) {
  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        .consumerSecret, .accessToken and .accessTokenSecret.
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def twitterStream(
      twitterAuth: Option[Authorization] = None,
      filters: Seq[String] = Nil,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[Status] = {
    val inputStream = new TwitterInputDStream(ssc, twitterAuth, filters, storageLevel)
    ssc.registerInputStream(inputStream)
    inputStream
  }
}
