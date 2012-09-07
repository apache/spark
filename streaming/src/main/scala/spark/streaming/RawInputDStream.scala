package spark.streaming

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch._
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, SocketChannel}
import java.io.EOFException
import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import spark.{DaemonThread, Logging, SparkEnv}
import spark.storage.StorageLevel

/**
 * An input stream that reads blocks of serialized objects from a given network address.
 * The blocks will be inserted directly into the block store. This is the fastest way to get
 * data into Spark Streaming, though it requires the sender to batch data and serialize it
 * in the format that the system is configured with.
 */
class RawInputDStream[T: ClassManifest](
    @transient ssc: StreamingContext,
    host: String,
    port: Int,
    storageLevel: StorageLevel)
  extends NetworkInputDStream[T](ssc) with Logging {

  val streamId = id

  /** Called on workers to run a receiver for the stream. */
  def runReceiver() {
    val env = SparkEnv.get
    val actor = env.actorSystem.actorOf(
      Props(new ReceiverActor(env, Thread.currentThread)), "ReceiverActor-" + streamId)

    // Open a socket to the target address and keep reading from it
    logInfo("Connecting to " + host + ":" + port)
    val channel = SocketChannel.open()
    channel.configureBlocking(true)
    channel.connect(new InetSocketAddress(host, port))
    logInfo("Connected to " + host + ":" + port)

    val queue = new ArrayBlockingQueue[ByteBuffer](2)

    new DaemonThread {
      override def run() {
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          val blockId = "input-" + streamId + "-" + nextBlockNumber
          nextBlockNumber += 1
          env.blockManager.putBytes(blockId, buffer, storageLevel)
          actor ! BlockPublished(blockId)
        }
      }
    }.start()

    val lengthBuffer = ByteBuffer.allocate(4)
    while (true) {
      lengthBuffer.clear()
      readFully(channel, lengthBuffer)
      lengthBuffer.flip()
      val length = lengthBuffer.getInt()
      val dataBuffer = ByteBuffer.allocate(length)
      readFully(channel, dataBuffer)
      dataBuffer.flip()
      logInfo("Read a block with " + length + " bytes")
      queue.put(dataBuffer)
    }
  }

  /** Read a buffer fully from a given Channel */
  private def readFully(channel: ReadableByteChannel, dest: ByteBuffer) {
    while (dest.position < dest.limit) {
      if (channel.read(dest) == -1) {
        throw new EOFException("End of channel")
      }
    }
  }

  /** Message sent to ReceiverActor to tell it that a block was published */
  case class BlockPublished(blockId: String) {}

  /** A helper actor that communicates with the NetworkInputTracker */
  private class ReceiverActor(env: SparkEnv, receivingThread: Thread) extends Actor {
    val newBlocks = new ArrayBuffer[String]

    logInfo("Attempting to register with tracker")
    val ip = System.getProperty("spark.master.host", "localhost")
    val port = System.getProperty("spark.master.port", "7077").toInt
    val actorName: String = "NetworkInputTracker"
    val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
    val trackerActor = env.actorSystem.actorFor(url)
    val timeout = 5.seconds

    override def preStart() {
      val future = trackerActor.ask(RegisterReceiver(streamId, self))(timeout)
      Await.result(future, timeout)
    }

    override def receive = {
      case BlockPublished(blockId) =>
        newBlocks += blockId
        val future = trackerActor ! GotBlockIds(streamId, Array(blockId))

      case GetBlockIds(time) =>
        logInfo("Got request for block IDs for " + time)
        sender ! GotBlockIds(streamId, newBlocks.toArray)
        newBlocks.clear()

      case StopReceiver =>
        receivingThread.interrupt()
        sender ! true
    }

  }
}
