package spark.streaming.examples

import spark.util.IntParam
import spark.SparkContext
import spark.SparkContext._
import spark.storage.StorageLevel
import spark.streaming._
import spark.streaming.StreamingContext._

import WordCount2_ExtraFunctions._

object WordCountRaw {
  def moreWarmup(sc: SparkContext) {
    (0 until 40).foreach {i =>
      sc.parallelize(1 to 20000000, 1000)
        .map(_ % 1331).map(_.toString)
        .mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
        .collect()
    }
  }

  def main(args: Array[String]) {
    if (args.length != 7) {
      System.err.println("Usage: WordCountRaw <master> <streams> <host> <port> <batchMs> <chkptMs> <reduces>")
      System.exit(1)
    }

    val Array(master, IntParam(streams), host, IntParam(port), IntParam(batchMs),
              IntParam(chkptMs), IntParam(reduces)) = args

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "WordCountRaw")
    ssc.setBatchDuration(Milliseconds(batchMs))

    // Make sure some tasks have started on each node
    moreWarmup(ssc.sc)

    val rawStreams = (1 to streams).map(_ =>
      ssc.createRawNetworkStream[String](host, port, StorageLevel.MEMORY_ONLY_2)).toArray
    val union = new UnifiedDStream(rawStreams)

    val windowedCounts = union.mapPartitions(splitAndCountPartitions)
      .reduceByKeyAndWindow(add _, subtract _, Seconds(30), Milliseconds(batchMs), reduces)
    windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.MEMORY_ONLY_DESER_2,
      Milliseconds(chkptMs))
    //windowedCounts.print()    // TODO: something else?
    windowedCounts.foreachRDD(r => println("Element count: " + r.count()))

    ssc.start()
  }
}
