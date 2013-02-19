package spark.streaming.examples.twitter

import spark.streaming.{Seconds, StreamingContext}
import spark.storage.StorageLevel
import com.twitter.algebird.HyperLogLog._
import com.twitter.algebird.HyperLogLogMonoid
import spark.streaming.dstream.TwitterInputDStream

/**
 * Example of using HyperLogLog monoid from Twitter's Algebird together with Spark Streaming's
 * TwitterInputDStream
 */
object StreamingHLL {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TwitterStreamingHLL <master> <twitter_username> <twitter_password>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    val Array(master, username, password) = args.slice(0, 3)
    val filters = args.slice(3, args.length)

    val ssc = new StreamingContext(master, "TwitterStreamingHLL", Seconds(2))
    val stream = new TwitterInputDStream(ssc, username, password, filters,
      StorageLevel.MEMORY_ONLY_SER)
    ssc.registerInputStream(stream)

    val users = stream.map(status => status.getUser.getId)

    val globalHll = new HyperLogLogMonoid(12)
    var userSet: Set[Long] = Set()

    val approxUsers = users.mapPartitions(ids => {
      val hll = new HyperLogLogMonoid(12)
      ids.map(id => hll(id))
    }).reduce(_ + _)

    val exactUsers = users.map(id => Set(id)).reduce(_ ++ _)

    var h = globalHll.zero
    approxUsers.foreach(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        h += partial
        println("Approx distinct users this batch: %d".format(partial.estimatedSize.toInt))
        println("Approx distinct users overall: %d".format(globalHll.estimateSize(h).toInt))
      }
    })

    exactUsers.foreach(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        userSet ++= partial
        println("Exact distinct users this batch: %d".format(partial.size))
        println("Exact distinct users overall: %d".format(userSet.size))
        println("Error rate: %2.5f%%".format(((globalHll.estimateSize(h) / userSet.size.toDouble) - 1) * 100))
      }
    })

    ssc.start()
  }
}
