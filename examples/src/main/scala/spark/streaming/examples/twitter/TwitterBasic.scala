package spark.streaming.examples.twitter

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._

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
    val stream = new TwitterInputDStream(ssc, username, password, filters)
    ssc.graph.addInputStream(stream)

    val hashTags = stream.flatMap(
      status => status.getText.split(" ").filter(_.startsWith("#")))

    // Word count over hashtags
    val counts = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    // TODO: Sorts on one node - should do with global sorting once streaming supports it
    counts.foreach(rdd => {
      val topList = rdd.collect().sortBy(-_._2).take(5)
      if (!topList.isEmpty) {
        println("\nPopular topics in last 60 seconds:")
        topList.foreach{case (tag, count) => println("%s (%s tweets)".format(tag, count))}
      }
    })

    // Print number of tweets in the window
    stream.window(Seconds(60)).count().foreach(rdd =>
      if (rdd.count() != 0) {
        println("Window size: %s tweets".format(rdd.take(1)(0)))
      }
    )
    ssc.start()
  }
}
