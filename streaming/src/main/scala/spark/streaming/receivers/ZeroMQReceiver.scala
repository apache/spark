package spark.streaming.receivers

import akka.actor.Actor
import akka.zeromq._

import spark.Logging

/**
 * A receiver to subscribe to ZeroMQ stream.
 */
private[streaming] class ZeroMQReceiver[T: ClassManifest](publisherUrl: String,
  subscribe: Subscribe,
  bytesToObjects: Seq[Seq[Byte]] ⇒ Iterator[T])
  extends Actor with Receiver with Logging {

  override def preStart() = context.system.newSocket(SocketType.Sub, Listener(self),
    Connect(publisherUrl), subscribe)

  def receive: Receive = {

    case Connecting ⇒ logInfo("connecting ...")

    case m: ZMQMessage ⇒
      logDebug("Received message for:" + m.firstFrameAsString)

      //We ignore first frame for processing as it is the topic
      val bytes = m.frames.tail.map(_.payload)
      pushBlock(bytesToObjects(bytes))

    case Closed ⇒ logInfo("received closed ")

  }
}
