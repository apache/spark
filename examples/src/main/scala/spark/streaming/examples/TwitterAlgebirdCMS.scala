package spark.streaming.examples

import spark.streaming.{Seconds, StreamingContext}
import spark.storage.StorageLevel
import com.twitter.algebird._
import spark.streaming.StreamingContext._
import spark.SparkContext._

/**
 * Example of using CountMinSketch monoid from Twitter's Algebird together with Spark Streaming's
 * TwitterInputDStream
 */
object TwitterAlgebirdCMS {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TwitterAlgebirdCMS <master> <twitter_username> <twitter_password>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    val DELTA = 1E-3
    val EPS = 0.01
    val SEED = 1
    val PERC = 0.001
    val TOPK = 10

    val Array(master, username, password) = args.slice(0, 3)
    val filters = args.slice(3, args.length)

    val ssc = new StreamingContext(master, "TwitterAlgebirdCMS", Seconds(10))
    val stream = ssc.twitterStream(username, password, filters,
      StorageLevel.MEMORY_ONLY_SER)

    val users = stream.map(status => status.getUser.getId)

    var globalCMS = new CountMinSketchMonoid(DELTA, EPS, SEED, PERC).zero
    var globalExact = Map[Long, Int]()
    val mm = new MapMonoid[Long, Int]()

    val approxTopUsers = users.mapPartitions(ids => {
      val cms = new CountMinSketchMonoid(DELTA, EPS, SEED, PERC)
      ids.map(id => cms.create(id))
    }).reduce(_ ++ _)

    val exactTopUsers = users.map(id => (id, 1))
      .reduceByKey((a, b) => a + b)

    approxTopUsers.foreach(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        val partialTopK = partial.heavyHitters.map(id =>
          (id, partial.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        globalCMS ++= partial
        val globalTopK = globalCMS.heavyHitters.map(id =>
          (id, globalCMS.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        println("Approx heavy hitters at %2.2f%% threshold this batch: %s".format(PERC,
          partialTopK.mkString("[", ",", "]")))
        println("Approx heavy hitters at %2.2f%% threshold overall: %s".format(PERC,
          globalTopK.mkString("[", ",", "]")))
      }
    })

    exactTopUsers.foreach(rdd => {
      if (rdd.count() != 0) {
        val partialMap = rdd.collect().toMap
        val partialTopK = rdd.map(
          {case (id, count) => (count, id)})
          .sortByKey(ascending = false).take(TOPK)
        globalExact = mm.plus(globalExact.toMap, partialMap)
        val globalTopK = globalExact.toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        println("Exact heavy hitters this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("Exact heavy hitters overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })

    ssc.start()
  }
}
