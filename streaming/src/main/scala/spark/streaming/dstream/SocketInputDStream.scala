package spark.streaming.dstream

import spark.streaming.StreamingContext
import spark.storage.StorageLevel
import spark.util.NextIterator

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

  def getReceiver(): NetworkReceiver[T] = {
    new SocketReceiver(host, port, bytesToObjects, storageLevel)
  }
}

private[streaming]
class SocketReceiver[T: ClassManifest](
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends NetworkReceiver[T] {

  lazy protected val blockGenerator = new BlockGenerator(storageLevel)

  override def getLocationPreference = None

  protected def onStart() {
    logInfo("Connecting to " + host + ":" + port)
    val socket = new Socket(host, port)
    logInfo("Connected to " + host + ":" + port)
    blockGenerator.start()
    val iterator = bytesToObjects(socket.getInputStream())
    while(iterator.hasNext) {
      val obj = iterator.next
      blockGenerator += obj
    }
  }

  protected def onStop() {
    blockGenerator.stop()
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
    new NextIterator[String] {
      protected override def getNext() = {
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close() {
        dataInputStream.close()
      }
    }
  }
}
