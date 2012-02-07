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
        try {
          val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, reduceId)
          // TODO: multithreaded fetch
          // TODO: would be nice to retry multiple times
          val inputStream = ser.inputStream(
              new FastBufferedInputStream(new URL(url).openStream()))
          try {
            while (true) {
              val pair = inputStream.readObject().asInstanceOf[(K, V)]
              func(pair._1, pair._2)
            }
          } finally {
            inputStream.close()
          }
        } catch {
          case e: EOFException => {} // We currently assume EOF means we read the whole thing
          case other: Exception => {
            logError("Fetch failed", other)
            throw new FetchFailedException(serverUri, shuffleId, i, reduceId, other)
          }
        }
      }
    }
  }
}
