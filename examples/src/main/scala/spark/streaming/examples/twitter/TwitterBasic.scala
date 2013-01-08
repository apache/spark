package spark.streaming.examples.twitter

import spark.streaming.StreamingContext._
import spark.streaming.{Seconds, StreamingContext}
import spark.SparkContext._
import spark.storage.StorageLevel

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 */
object TwitterBasic {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TwitterBasic <master> <twitter_username> <twitter_password>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    val Array(master, username, password) = args.slice(0, 3)
    val filters = args.slice(3, args.length)

    val ssc = new StreamingContext(master, "TwitterBasic", Seconds(2))
    val stream = new TwitterInputDStream(ssc, username, password, filters,
      StorageLevel.MEMORY_ONLY_SER)
    ssc.registerInputStream(stream)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreach(rdd => {
      if (rdd.count() != 0) {
        val topList = rdd.take(5)
        println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      }
    })

    topCounts10.foreach(rdd => {
      if (rdd.count() != 0) {
        val topList = rdd.take(5)
        println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      }
    })

    ssc.start()
  }

}
