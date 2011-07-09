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

import org.eclipse.jetty.client.ContentExchange
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.HttpExchange
import org.eclipse.jetty.util.thread.QueuedThreadPool


class ParallelShuffleFetcher extends ShuffleFetcher with Logging {
  val parallelFetches = System.getProperty("spark.parallel.fetches", "3").toInt
  val httpClient = createHttpClient()

  private def createHttpClient(): HttpClient = {
    val client = new HttpClient
    client.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    client.setThreadPool(threadPool);
    client.setTimeout(30000);
    client.start();
    client
  }

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
              var exception: Throwable = null
              val exchange = new ContentExchange(true) {
                override def onException(e: Throwable) { exception = e }
              }
              exchange.setURL(url)
              httpClient.send(exchange)
              val status = exchange.waitForDone()
              logInfo("Finished HTTP request for " + url + ", status = " + status)
              status match {
                case HttpExchange.STATUS_COMPLETED =>
                  resultQueue.put(exchange.getResponseContentBytes())
                case HttpExchange.STATUS_EXCEPTED =>
                  logError("Fetch failed from " + url + " with exception", exception)
                  failure.set(new FetchFailedException(serverUri, shuffleId, i, reduceId, exception))
                  return
                case HttpExchange.STATUS_EXPIRED =>
                  logError("Fetch failed from " + url + " with expired status (timeout)")
                  failure.set(new FetchFailedException(serverUri, shuffleId, i, reduceId, null))
                  return
                case other =>
                  logError("Fetch failed from " + url + " with unknown Jetty status " + other)
                  failure.set(new FetchFailedException(serverUri, shuffleId, i, reduceId, null))
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
      } catch {case e: InterruptedException => {}}
    }
    if (failure.get != null) {
      throw failure.get
    }
  }

  override def stop() {
    httpClient.stop()
  }
}
