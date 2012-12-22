package spark.streaming

import spark.RDD
import spark.streaming.{Time, InputDStream}
import twitter4j._
import twitter4j.auth.BasicAuthorization
import collection.mutable.ArrayBuffer
import collection.JavaConversions._

/* A stream of Twitter statuses, potentially filtered by one or more keywords.
*
* @constructor create a new Twitter stream using the supplied username and password to authenticate.
* An optional set of string filters can be used to restrict the set of tweets. The Twitter API is
* such that this may return a sampled subset of all tweets during each interval.
*/
class TwitterInputDStream(
    @transient ssc_ : StreamingContext,
    username: String,
    password: String,
    filters: Seq[String]
    ) extends InputDStream[Status](ssc_)  {
  val statuses: ArrayBuffer[Status] = ArrayBuffer()
  var twitterStream: TwitterStream = _

  override def start() = {
    twitterStream = new TwitterStreamFactory()
      .getInstance(new BasicAuthorization(username, password))
    twitterStream.addListener(new StatusListener {
      def onStatus(status: Status) = {
        statuses += status
      }
      // Unimplemented
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(i: Int) {}
      def onScrubGeo(l: Long, l1: Long) {}
      def onStallWarning(stallWarning: StallWarning) {}
      def onException(e: Exception) {}
    })

    val query: FilterQuery = new FilterQuery
    if (filters.size > 0) {
      query.track(filters.toArray)
      twitterStream.filter(query)
    } else {
      twitterStream.sample()
    }
  }

  override def stop() = {
    twitterStream.shutdown()
  }

  override def compute(validTime: Time): Option[RDD[Status]] = {
    // Flush the current tweet buffer
    val rdd = Some(ssc.sc.parallelize(statuses))
    statuses.foreach(x => statuses -= x)
    rdd
  }
}
