package spark.network

import java.nio.ByteBuffer
import java.net.InetAddress

private[spark] object ReceiverTest {

  def main(args: Array[String]) {
    val manager = new ConnectionManager(9999)
    println("Started connection manager with id = " + manager.id)
    
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => { 
      /*println("Received [" + msg + "] from [" + id + "] at " + System.currentTimeMillis)*/
      val buffer = ByteBuffer.wrap("response".getBytes())
      Some(Message.createBufferMessage(buffer, msg.id))
    })
    Thread.currentThread.join()  
  }
}

