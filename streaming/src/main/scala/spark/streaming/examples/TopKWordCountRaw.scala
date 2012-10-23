package spark.streaming.examples

import spark.util.IntParam
import spark.SparkContext
import spark.SparkContext._
import spark.storage.StorageLevel
import spark.streaming._
import spark.streaming.StreamingContext._

import WordCount2_ExtraFunctions._

object TopKWordCountRaw {
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
      System.err.println("Usage: TopKWordCountRaw <master> <streams> <host> <port> <batchMs> <chkptMs> <reduces>")
      System.exit(1)
    }

    val Array(master, IntParam(streams), host, IntParam(port), IntParam(batchMs),
              IntParam(chkptMs), IntParam(reduces)) = args

    // Create the context and set the batch size
    val ssc = new StreamingContext(master, "TopKWordCountRaw")
    ssc.setBatchDuration(Milliseconds(batchMs))

    // Make sure some tasks have started on each node
    moreWarmup(ssc.sc)

    val rawStreams = (1 to streams).map(_ =>
      ssc.rawNetworkStream[String](host, port, StorageLevel.MEMORY_ONLY_2)).toArray
    val union = new UnionDStream(rawStreams)

    val windowedCounts = union.mapPartitions(splitAndCountPartitions)
      .reduceByKeyAndWindow(add _, subtract _, Seconds(30), Milliseconds(batchMs), reduces)
    windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.MEMORY_ONLY_DESER_2,
      Milliseconds(chkptMs))
    //windowedCounts.print()    // TODO: something else?

    def topK(data: Iterator[(String, Long)], k: Int): Iterator[(String, Long)] = {
      val taken = new Array[(String, Long)](k)
      
      var i = 0
      var len = 0
      var done = false
      var value: (String, Long) = null
      var swap: (String, Long) = null
      var count = 0

      while(data.hasNext) {
        value = data.next
        count += 1
        println("count = " + count)
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) {
          if (len < k) {
            len += 1
          }
          taken(len - 1) = value
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
      println("Took " + len + " out of " + count + " items")
      return taken.toIterator  
    }
    
    val k = 50
    val partialTopKWindowedCounts = windowedCounts.mapPartitions(topK(_, k))
    partialTopKWindowedCounts.foreachRDD(rdd => {
      val collectedCounts = rdd.collect
      println("Collected " + collectedCounts.size + " items")
      topK(collectedCounts.toIterator, k).foreach(println)
    })

//    windowedCounts.foreachRDD(r => println("Element count: " + r.count()))

    ssc.start()
  }
}
