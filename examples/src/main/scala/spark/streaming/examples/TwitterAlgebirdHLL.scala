package spark.streaming.examples

import spark.streaming.{Seconds, StreamingContext}
import spark.storage.StorageLevel
import com.twitter.algebird.HyperLogLog._
import com.twitter.algebird.HyperLogLogMonoid
import spark.streaming.dstream.TwitterInputDStream

/**
 * Example using HyperLogLog monoid from Twitter's Algebird together with Spark Streaming's
 * TwitterInputDStream to compute approximate distinct counts of userids.
 */
object TwitterAlgebirdHLL {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TwitterAlgebirdHLL <master> <twitter_username> <twitter_password>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    /** Bit size parameter for HyperLogLog */
    val BIT_SIZE = 12
    val Array(master, username, password) = args.slice(0, 3)
    val filters = args.slice(3, args.length)

    val ssc = new StreamingContext(master, "TwitterAlgebirdHLL", Seconds(5))
    val stream = ssc.twitterStream(username, password, filters, StorageLevel.MEMORY_ONLY_SER)

    val users = stream.map(status => status.getUser.getId)

    var globalHll = new HyperLogLogMonoid(BIT_SIZE).zero
    var userSet: Set[Long] = Set()

    val approxUsers = users.mapPartitions(ids => {
      val hll = new HyperLogLogMonoid(BIT_SIZE)
      ids.map(id => hll(id))
    }).reduce(_ + _)

    val exactUsers = users.map(id => Set(id)).reduce(_ ++ _)

    approxUsers.foreach(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        globalHll += partial
        println("Approx distinct users this batch: %d".format(partial.estimatedSize.toInt))
        println("Approx distinct users overall: %d".format(globalHll.estimatedSize.toInt))
      }
    })

    exactUsers.foreach(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        userSet ++= partial
        println("Exact distinct users this batch: %d".format(partial.size))
        println("Exact distinct users overall: %d".format(userSet.size))
        println("Error rate: %2.5f%%".format(((globalHll.estimatedSize / userSet.size.toDouble) - 1) * 100))
      }
    })

    ssc.start()
  }
}
