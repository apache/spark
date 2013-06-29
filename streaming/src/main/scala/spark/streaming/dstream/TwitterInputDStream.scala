package spark.streaming.dstream

import spark._
import spark.streaming._
import storage.StorageLevel
import twitter4j._
import twitter4j.auth.BasicAuthorization
import twitter4j.auth.Authorization
import java.util.prefs.Preferences
import twitter4j.conf.PropertyConfiguration
import twitter4j.auth.OAuthAuthorization
import twitter4j.auth.AccessToken

/* A stream of Twitter statuses, potentially filtered by one or more keywords.
*
* @constructor create a new Twitter stream using the supplied username and password to authenticate.
* An optional set of string filters can be used to restrict the set of tweets. The Twitter API is
* such that this may return a sampled subset of all tweets during each interval.
* 
* Includes a simple implementation of OAuth using consumer key and secret provided using system 
* properties twitter4j.oauth.consumerKey and twitter4j.oauth.consumerSecret
*/
private[streaming]
class TwitterInputDStream(
    @transient ssc_ : StreamingContext,
    twitterAuth: Option[Authorization],
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[Status](ssc_)  {
  
  lazy val createOAuthAuthorization: Authorization = {
    val userRoot = Preferences.userRoot();
    val token = Option(userRoot.get(PropertyConfiguration.OAUTH_ACCESS_TOKEN, null))
    val tokenSecret = Option(userRoot.get(PropertyConfiguration.OAUTH_ACCESS_TOKEN_SECRET, null))
    val oAuth = new OAuthAuthorization(new PropertyConfiguration(System.getProperties()))
    if (token.isEmpty || tokenSecret.isEmpty) {
      val requestToken = oAuth.getOAuthRequestToken()
	  println("Authorize application using URL: "+requestToken.getAuthorizationURL())
	  println("Enter PIN: ")
      val pin = Console.readLine
      val accessToken = if (pin.length() > 0) oAuth.getOAuthAccessToken(requestToken, pin) else oAuth.getOAuthAccessToken()
      userRoot.put(PropertyConfiguration.OAUTH_ACCESS_TOKEN, accessToken.getToken())
      userRoot.put(PropertyConfiguration.OAUTH_ACCESS_TOKEN_SECRET, accessToken.getTokenSecret())
      userRoot.flush()
    } else {
      oAuth.setOAuthAccessToken(new AccessToken(token.get, tokenSecret.get));
    }
    oAuth
  }
  
  override def getReceiver(): NetworkReceiver[Status] = {
    new TwitterReceiver(if (twitterAuth.isEmpty) createOAuthAuthorization else twitterAuth.get, filters, storageLevel)
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
