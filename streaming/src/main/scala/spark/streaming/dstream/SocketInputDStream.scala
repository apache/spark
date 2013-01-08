package spark.streaming.dstream

import spark.streaming.StreamingContext
import spark.storage.StorageLevel

import java.io._
import java.net.Socket

private[streaming]
class SocketInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_) {

  def createReceiver(): NetworkReceiver[T] = {
    new SocketReceiver(id, host, port, bytesToObjects, storageLevel)
  }
}

private[streaming]
class SocketReceiver[T: ClassManifest](
    streamId: Int,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends NetworkReceiver[T](streamId) {

  lazy protected val dataHandler = new DataHandler(this, storageLevel)

  override def getLocationPreference = None

  protected def onStart() {
    logInfo("Connecting to " + host + ":" + port)
    val socket = new Socket(host, port)
    logInfo("Connected to " + host + ":" + port)
    dataHandler.start()
    val iterator = bytesToObjects(socket.getInputStream())
    while(iterator.hasNext) {
      val obj = iterator.next
      dataHandler += obj
    }
  }

  protected def onStop() {
    dataHandler.stop()
  }

}

private[streaming]
object SocketReceiver  {

  /**
   * This methods translates the data from an inputstream (say, from a socket)
   * to '\n' delimited strings and returns an iterator to access the strings.
   */
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))

    val iterator = new Iterator[String] {
      var gotNext = false
      var finished = false
      var nextValue: String = null

      private def getNext() {
        try {
          nextValue = dataInputStream.readLine()
          if (nextValue == null) {
            finished = true
          }
        }
        gotNext = true
      }

      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            getNext()
            if (finished) {
              dataInputStream.close()
            }
          }
        }
        !finished
      }

      override def next(): String = {
        if (finished) {
          throw new NoSuchElementException("End of stream")
        }
        if (!gotNext) {
          getNext()
        }
        gotNext = false
        nextValue
      }
    }
    iterator
  }
}
