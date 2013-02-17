package spark.streaming.dstream

import spark._
import spark.streaming._
import storage.StorageLevel

import twitter4j._
import twitter4j.auth.BasicAuthorization

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
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[Status](ssc_)  {

  override def createReceiver(): NetworkReceiver[Status] = {
    new TwitterReceiver(username, password, filters, storageLevel)
  }
}

class TwitterReceiver(
    username: String,
    password: String,
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkReceiver[Status] {

  var twitterStream: TwitterStream = _
  lazy val blockGenerator = new BlockGenerator(storageLevel)

  protected override def onStart() {
    blockGenerator.start()
    twitterStream = new TwitterStreamFactory()
      .getInstance(new BasicAuthorization(username, password))
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
