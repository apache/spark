package spark.streaming.examples

import spark.streaming.StreamingContext._
import spark.streaming.{TwitterInputDStream, Seconds, StreamingContext}

object TwitterBasic {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: TwitterBasic <master> <twitter_username> <twitter_password>")
      System.exit(1)
    }

    val Array(master, username, password) = args

    val ssc = new StreamingContext(master, "TwitterBasic", Seconds(2))
    val stream = new TwitterInputDStream(ssc, username, password, Seq())
    ssc.graph.addInputStream(stream)

    val hashTags = stream.flatMap(
      status => status.getText.split(" ").filter(_.startsWith("#")))

    // Word count over hashtags
    val counts = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))

    // TODO: Sorts on one node - should do with global sorting once streaming supports it
    val topCounts = counts.collect().map(_.sortBy(-_._2).take(5))

    topCounts.foreachRDD(rdd => {
      val topList = rdd.take(1)(0)
      println("\nPopular topics in last 60 seconds:")
      topList.foreach(t => println("%s (%s tweets)".format(t._1, t._2)))
    }
    )

    ssc.start()
  }
}
