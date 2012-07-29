package spark.stream
import java.net.{Socket, ServerSocket}
import java.io.{ByteArrayOutputStream, DataOutputStream, DataInputStream, BufferedInputStream}

object Receiver {
  def main(args: Array[String]) {
    val port = args(0).toInt
    val lsocket = new ServerSocket(port)
    println("Listening on port " + port )
    while(true) {
      val socket = lsocket.accept()
      (new Thread() {
        override def run() {
          val buffer = new Array[Byte](100000)
          var count = 0
          val time = System.currentTimeMillis
          try {
            val is = new DataInputStream(new BufferedInputStream(socket.getInputStream))
            var loop = true
            var string: String = null
            while((string = is.readUTF) != null) {
              count += 28 
            }
          } catch {
            case e: Exception => e.printStackTrace
          }
          val timeTaken = System.currentTimeMillis - time
          val tput = (count / 1024.0) / (timeTaken / 1000.0)
          println("Data = " + count + " bytes\nTime = " + timeTaken + " ms\nTput = " + tput + " KB/s")
        }
      }).start()
    }
  }

}

object Sender {
  
  def main(args: Array[String]) {
    try {
      val host = args(0)
      val port = args(1).toInt
      val size = args(2).toInt
      
      val byteStream = new ByteArrayOutputStream()
      val stringDataStream = new DataOutputStream(byteStream)
      (0 until size).foreach(_ => stringDataStream.writeUTF("abcdedfghijklmnopqrstuvwxy"))
      val bytes = byteStream.toByteArray()
      println("Generated array of " + bytes.length + " bytes")

      /*val bytes = new Array[Byte](size)*/
      val socket = new Socket(host, port)
      val os = socket.getOutputStream
      os.write(bytes)
      os.flush
      socket.close()
      
    } catch {
      case e: Exception => e.printStackTrace
    }
  }
}

