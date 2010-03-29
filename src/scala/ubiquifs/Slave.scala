package ubiquifs

import java.io.{DataInputStream, DataOutputStream, IOException}
import java.net.{InetAddress, Socket, ServerSocket}
import java.util.concurrent.locks.ReentrantLock

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.remote.RemoteActor
import scala.actors.remote.RemoteActor._
import scala.actors.remote.Node
import scala.collection.mutable.{ArrayBuffer, Map, Set}

class Slave(myPort: Int, master: String) extends Thread("UbiquiFS slave") {
  val CHUNK_SIZE = 1024 * 1024

  val buffers = Map[String, Buffer]()

  override def run() {
    // Create server socket
    val socket = new ServerSocket(myPort)

    // Register with master
    val (masterHost, masterPort) = Utils.parseHostPort(master)
    val masterActor = select(Node(masterHost, masterPort), 'UbiquiFS)
    val myHost = InetAddress.getLocalHost.getHostName
    val reply = masterActor !? RegisterSlave(myHost, myPort)
    println("Registered with master, reply = " + reply)

    while (true) {
      val conn = socket.accept()
      new ConnectionHandler(conn).start()
    }
  }

  class ConnectionHandler(conn: Socket) extends Thread("ConnectionHandler") {
    try {
      val in = new DataInputStream(conn.getInputStream)
      val out = new DataOutputStream(conn.getOutputStream)
      val header = Header.read(in)
      header.requestType match {
        case RequestType.READ =>
          performRead(header.path, out)
        case RequestType.WRITE =>
          performWrite(header.path, in)
        case other =>
          throw new IOException("Invalid header type " + other)
      }
      println("hi")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      conn.close()
    }
  }

  def performWrite(path: String, in: DataInputStream) {
    var buffer = new Buffer()
    synchronized {
      if (buffers.contains(path))
        throw new IllegalArgumentException("Path " + path + " already exists")
      buffers(path) = buffer
    }
    var chunk = new Array[Byte](CHUNK_SIZE)
    var pos = 0
    while (true) {
      var numRead = in.read(chunk, pos, chunk.size - pos)
      if (numRead == -1) {
        buffer.addChunk(chunk.subArray(0, pos), true)
        return
      } else {
        pos += numRead
        if (pos == chunk.size) {
          buffer.addChunk(chunk, false)
          chunk = new Array[Byte](CHUNK_SIZE)
          pos = 0
        }
      }
    }
    // TODO: launch a thread to write the data to disk, and when this finishes,
    // remove the hard reference to buffer
  }

  def performRead(path: String, out: DataOutputStream) {
    var buffer: Buffer = null
    synchronized {
      if (!buffers.contains(path))
        throw new IllegalArgumentException("Path " + path + " doesn't exist")
      buffer = buffers(path)
    }
    for (chunk <- buffer.iterator) {
      out.write(chunk, 0, chunk.size)
    }
  }

  class Buffer {
    val chunks = new ArrayBuffer[Array[Byte]]
    var finished = false
    val mutex = new ReentrantLock
    val chunksAvailable = mutex.newCondition()

    def addChunk(chunk: Array[Byte], finish: Boolean) {
      mutex.lock()
      chunks += chunk
      finished = finish
      chunksAvailable.signalAll()
      mutex.unlock()
    }

    def iterator = new Iterator[Array[Byte]] {
      var index = 0

      def hasNext: Boolean = {
        mutex.lock()
        while (index >= chunks.size && !finished)
          chunksAvailable.await()
        val ret = (index < chunks.size)
        mutex.unlock()
        return ret
      }

      def next: Array[Byte] = {
        mutex.lock()
        if (!hasNext)
          throw new NoSuchElementException("End of file")
        val ret = chunks(index) // hasNext ensures we advance past index
        index += 1
        mutex.unlock()
        return ret
      }
    }
  }
}

object SlaveMain {
  def main(args: Array[String]) {
    val port = args(0).toInt
    val master = args(1)
    new Slave(port, master).start()
  }
}
