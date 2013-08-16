package spark.storage

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong

import spark.{KryoSerializer, SparkContext, Utils}

/** Utility for micro-benchmarking storage performance. */
object StoragePerfTester {
  /** Writing shuffle data from several concurrent tasks and measure throughput. */
  def main(args: Array[String]) = {
    def intArg(key: String, default: Int) = Option(System.getenv(key)).map(_.toInt).getOrElse(default)
    def stringArg(key: String, default: String) = Option(System.getenv(key)).getOrElse(default)

    /** Total number of simulated shuffles to run. */
    val numShuffles = intArg("NUM_SHUFFLES", 1)

    /** Total amount of data to generate, will be distributed evenly amongst maps and reduce splits. */
    val dataSizeMb = Utils.memoryStringToMb(stringArg("OUTPUT_DATA", "1g"))

    /** Number of map tasks. All tasks execute concurrently. */
    val numMaps = intArg("NUM_MAPS", 8)

    /** Number of reduce splits for each map task. */
    val numOutputSplits = intArg("NUM_REDUCERS", 500)

    val recordLength = 1000 // ~1KB records
    val totalRecords = dataSizeMb * 1000
    val recordsPerMap = totalRecords / numMaps

    val writeData = "1" * recordLength
    val executor = Executors.newFixedThreadPool(numMaps)

    System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.shuffle.sync", "true")

    val sc = new SparkContext("local[4]", "Write Tester")
    val blockManager = sc.env.blockManager

    def writeOutputBytes(mapId: Int, shuffleId: Int, total: AtomicLong) = {
      val shuffle = blockManager.shuffleBlockManager.forShuffle(shuffleId, numOutputSplits, new KryoSerializer())
      val buckets = shuffle.acquireWriters(mapId)
      for (i <- 1 to recordsPerMap) {
        buckets.writers(i % numOutputSplits).write(writeData)
      }
      buckets.writers.map {w =>
        w.commit()
        total.addAndGet(w.size())
        w.close()
      }

      shuffle.releaseWriters(buckets)
    }

    for (shuffle <- 1 to numShuffles) {
      val start = System.currentTimeMillis()
      val latch = new CountDownLatch(numMaps)
      val totalBytes = new AtomicLong()
      for (task <- 1 to numMaps) {
        executor.submit(new Runnable() {
          override def run() = {
            try {
              writeOutputBytes(task, shuffle, totalBytes)
              latch.countDown()
            } catch {
              case e: Exception =>
                println("Exception in child thread: " + e + " " + e.getMessage)
                System.exit(1)
            }
          }
        })
      }
      latch.await()
      val end = System.currentTimeMillis()
      val time = (end - start) / 1000.0
      val bytesPerSecond = totalBytes.get() / time
      val bytesPerFile = (totalBytes.get() / (numOutputSplits * numMaps.toDouble)).toLong

      System.err.println("files_total\t\t%s".format(numMaps * numOutputSplits))
      System.err.println("bytes_per_file\t\t%s".format(Utils.memoryBytesToString(bytesPerFile)))
      System.err.println("agg_throughput\t\t%s/s".format(Utils.memoryBytesToString(bytesPerSecond.toLong)))
      System.err.println("Shuffle %s is finished in %ss. To run next shuffle, press Enter:".format(shuffle, time))
      readLine()
    }

    executor.shutdown()
    sc.stop()
  }
}
