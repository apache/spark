package spark

import java.io.EOFException
import java.net.URL

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

class SimpleShuffleFetcher extends ShuffleFetcher with Logging {
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit) {
    logInfo("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val ser = SparkEnv.get.serializer.newInstance()
    val splitsByUri = new HashMap[String, ArrayBuffer[Int]]
    val serverUris = SparkEnv.get.mapOutputTracker.getServerUris(shuffleId)
    for ((serverUri, index) <- serverUris.zipWithIndex) {
      splitsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
    }
    for ((serverUri, inputIds) <- Utils.randomize(splitsByUri)) {
      for (i <- inputIds) {
        val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, reduceId)
        var totalRecords = -1
        var recordsProcessed = 0
        var tries = 0
        while (totalRecords == -1 || recordsProcessed < totalRecords) {
          tries += 1
          if (tries > 4) {
            // We've tried four times to get this data but we've had trouble; let's just declare
            // a failed fetch
            logError("Failed to fetch " + url + " four times; giving up")
            throw new FetchFailedException(serverUri, shuffleId, i, reduceId, null)
          }
          var recordsRead = 0
          try {
            val inputStream = ser.inputStream(
                new FastBufferedInputStream(new URL(url).openStream()))
            try {
              totalRecords = inputStream.readObject().asInstanceOf[Int]
              logDebug("Total records to read from " + url + ": " + totalRecords)
              while (true) {
                val pair = inputStream.readObject().asInstanceOf[(K, V)]
                if (recordsRead <= recordsProcessed) {
                  func(pair._1, pair._2)
                  recordsProcessed += 1
                }
                recordsRead += 1
              }
            } finally {
              inputStream.close()
            }
          } catch {
            case e: EOFException => {
              logDebug("Reduce %s got %s records from map %s before EOF".format(
                reduceId, recordsRead, i))
              if (recordsRead < totalRecords) {
                logInfo("Reduce %s only got %s/%s records from map %s before EOF; retrying".format(
                  reduceId, recordsRead, totalRecords, i))
              }
            }
            case other: Exception => {
              logError("Fetch failed", other)
              throw new FetchFailedException(serverUri, shuffleId, i, reduceId, other)
            }
          }
        }
        logInfo("Fetched all " + totalRecords + " records successfully")
      }
    }
  }
}
