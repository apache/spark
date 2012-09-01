package spark.streaming

import java.io.InputStream

class ObjectInputDStream[T: ClassManifest](
    @transient ssc: StreamingContext,
    val host: String,
    val port: Int,
    val bytesToObjects: InputStream => Iterator[T])
  extends NetworkInputDStream[T](ssc) {

  override def runReceiver() {
    new ObjectInputReceiver(id, host, port, bytesToObjects).run()
  }
}

