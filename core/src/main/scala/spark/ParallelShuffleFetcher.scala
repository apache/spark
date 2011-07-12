package spark

import java.io.ByteArrayInputStream
import java.io.EOFException
import java.net.URL
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap


class ParallelShuffleFetcher extends ShuffleFetcher with Logging {
  val parallelFetches = System.getProperty("spark.parallel.fetches", "3").toInt

  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit) {
    logInfo("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    
    // Figure out a list of input IDs (mapper IDs) for each server
    val ser = SparkEnv.get.serializer.newInstance()
    val inputsByUri = new HashMap[String, ArrayBuffer[Int]]
    val serverUris = SparkEnv.get.mapOutputTracker.getServerUris(shuffleId)
    for ((serverUri, index) <- serverUris.zipWithIndex) {
      inputsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
    }
    
    // Randomize them and put them in a LinkedBlockingQueue
    val serverQueue = new LinkedBlockingQueue[(String, ArrayBuffer[Int])]
    for (pair <- Utils.randomize(inputsByUri))
      serverQueue.put(pair)

    // Create a queue to hold the fetched data
    val resultQueue = new LinkedBlockingQueue[Array[Byte]]

    // Atomic variables to communicate failures and # of fetches done
    var failure = new AtomicReference[FetchFailedException](null)

    // Start multiple threads to do the fetching (TODO: may be possible to do it asynchronously)
    for (i <- 0 until parallelFetches) {
      new Thread("Fetch thread " + i + " for reduce " + reduceId) {
        override def run() {
          while (true) {
            val pair = serverQueue.poll()
            if (pair == null)
              return
            val (serverUri, inputIds) = pair
            //logInfo("Pulled out server URI " + serverUri)
            for (i <- inputIds) {
              if (failure.get != null)
                return
              val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, reduceId)
              logInfo("Starting HTTP request for " + url)
              try {
                val conn = new URL(url).openConnection()
                conn.connect()
                val len = conn.getContentLength()
                if (len == -1)
                  throw new SparkException("Content length was not specified by server")
                val buf = new Array[Byte](len)
                val in = conn.getInputStream()
                var pos = 0
                while (pos < len) {
                  val n = in.read(buf, pos, len-pos)
                  if (n == -1)
                    throw new SparkException("EOF before reading the expected " + len + " bytes")
                  else
                    pos += n
                }
                // Done reading everything
                resultQueue.put(buf)
                in.close()
              } catch {
                case e: Exception =>
                  logError("Fetch failed from " + url, e)
                  failure.set(new FetchFailedException(serverUri, shuffleId, i, reduceId, e))
                  return
              }
            }
            //logInfo("Done with server URI " + serverUri)
          }
        }
      }.start()
    }

    // Wait for results from the threads (either a failure or all servers done)
    var resultsDone = 0
    var totalResults = inputsByUri.map{case (uri, inputs) => inputs.size}.sum
    while (failure.get == null && resultsDone < totalResults) {
      try {
        val result = resultQueue.poll(100, TimeUnit.MILLISECONDS)
        if (result != null) {
          //logInfo("Pulled out a result")
          val in = ser.inputStream(new ByteArrayInputStream(result))
            try {
            while (true) {
              val pair = in.readObject().asInstanceOf[(K, V)]
              func(pair._1, pair._2)
            }
          } catch {
            case e: EOFException => {} // TODO: cleaner way to detect EOF, such as a sentinel
          }
          resultsDone += 1
          //logInfo("Results done = " + resultsDone)
        }
      } catch { case e: InterruptedException => {} }
    }
    if (failure.get != null) {
      throw failure.get
    }
  }
}
