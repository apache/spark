/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.jms

import java.util.Properties
import java.util.concurrent._
import javax.jms._
import javax.naming.InitialContext

import akka.remote.WireFormats.AcknowledgementInfo
import com.google.common.base.Stopwatch
import org.apache.spark.Logging
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/**
  * Reliable receiver for a JMS source
  *
  * @param consumerFactory  Implementation specific factory for building MessageConsumer.
  *                         Use JndiMessageConsumerFactory to setup via JNDI
  * @param messageConverter Function to map from Message type to T. Return None to filter out
  *                         message
  * @param storageLevel
  * @tparam T
  */
abstract class BaseJmsReceiver[T](val consumerFactory: MessageConsumerFactory,
                                  val messageConverter: (Message) => Option[T],
                                  override val storageLevel: StorageLevel
                         = StorageLevel.MEMORY_AND_DISK_SER_2)
  extends Receiver[T](storageLevel) with Logging {

  @volatile
  @transient
  var receiverThread: Option[ExecutorService] = None

  @volatile
  @transient
  var running = false

  @volatile
  @transient
  var blockGenerator: BlockGenerator = _

  val stopWaitTime = 1000

  override def onStop(): Unit = {
    running = false
    consumerFactory.stopConnection()
    receiverThread.foreach(_.shutdown())
    receiverThread.foreach {
      ex =>
        if (!ex.awaitTermination(stopWaitTime, TimeUnit.MILLISECONDS)) {
          ex.shutdownNow()
        }
    }
    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }
  }
}

/**
  * Reliable Receiver to use for a Jms provider that does not support an individual acknowledgment
  * mode.
  *
  * @param consumerFactory  Implementation specific factory for building MessageConsumer.
  *                         Use JndiMessageConsumerFactory to setup via JNDI
  * @param messageConverter Function to map from Message type to T. Return None to filter out
  *                         message
  * @param batchSize        How meany messages to read off JMS source before submitting to
  *                         streaming. Every batch is a new task so reasonably high to avoid
  *                         excessive task creation.
  * @param maxWait          Max time to wait for messages before submitting a batch to streaming.
  * @param maxBatchAge      Max age of a batch before it is submitting. Used to cater for the case
  *                         of a slow trickle of messages
  * @param storageLevel
  * @tparam T
  */
class SynchronousJmsReceiver[T](override val consumerFactory: MessageConsumerFactory,
                                override val messageConverter: (Message) => Option[T],
                                val batchSize: Int = 10000,
                                val maxWait: Duration = 1.second,
                                val maxBatchAge: Duration = 1.seconds,
                                override val storageLevel: StorageLevel
                         = StorageLevel.MEMORY_AND_DISK_SER_2)
  extends BaseJmsReceiver[T](consumerFactory, messageConverter, storageLevel) {

  override def onStart(): Unit = {
    running = true
    receiverThread = Some(Executors.newSingleThreadExecutor())
    // We are just using the blockGenerator for access to rate limiter
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)
    receiverThread.get.execute {
      new Runnable {
        val buffer = ArrayBuffer[Message]()
        val stopWatch = new Stopwatch().start()

        override def run() = {
          try {
            val consumer = consumerFactory.newConsumer(Session.CLIENT_ACKNOWLEDGE)
            while (running) {
              if (buffer.size >= batchSize ||
                stopWatch.elapsed(TimeUnit.MILLISECONDS) >=
                  maxBatchAge.toMillis) {
                storeBuffer()
              }
              val message = if (maxWait.toMillis > 0) {
                consumer.receive(maxWait.toMillis)
              } else {
                consumer.receiveNoWait()
              }
              if (message == null && running) {
                storeBuffer()
              } else {
                blockGenerator.waitToPush() // Use rate limiter
                buffer += (message)
              }
            }
          } catch {
            case e: Throwable =>
              logError(e.getLocalizedMessage, e)
              restart(e.getLocalizedMessage, e)
          }
        }
        def storeBuffer() = {
          if (buffer.nonEmpty) {
            store(buffer.flatMap(x => messageConverter(x)))
            buffer.last.acknowledge()
            buffer.clear()
          }
          stopWatch.reset()
          stopWatch.start()
        }
      }
    }
  }

  class GeneratedBlockHandler extends BlockGeneratorListener {

    override def onAddData(data: Any, metadata: Any): Unit = {}

    override def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {}

    override def onError(message: String, throwable: Throwable): Unit = {}

    override def onGenerateBlock(blockId: StreamBlockId): Unit = {}
  }

}

/**
  * Jms receiver that support asynchronous acknowledgement. If used with an individual
  * acknowledgement mode can be considered "Reliable". Individual acknowledgement mode is not
  * currently part of JMS spec but is supported by some vendors such as ActiveMQ and
  * Solace
  *
  * @param consumerFactory     Implementation specific factory for building MessageConsumer.
  *                            Use JndiMessageConsumerFactory to setup via JNDI
  * @param messageConverter    Function to map from Message type to T. Return None to filter out
  *                            message
  * @param acknowledgementMode Should either be Session.AUTO_ACKNOWLEDGE or a JMS providers code
  *                            for individual acknowledgement. If set to Session.AUTO_ACKNOWLEDGE
  *                            then this receiver is not "Reliable"
  * @param storageLevel
  * @tparam T
  */
class AsynchronousJmsReceiver[T](override val consumerFactory: MessageConsumerFactory,
                                 override val messageConverter: (Message) => Option[T],
                                 val acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE,
                                 override val storageLevel: StorageLevel
                          = StorageLevel.MEMORY_AND_DISK_SER_2)
  extends BaseJmsReceiver[T](consumerFactory, messageConverter, storageLevel) {
  override def onStart(): Unit = {
    running = true
    receiverThread = Some(Executors.newSingleThreadExecutor())
    blockGenerator = supervisor.createBlockGenerator(new AsyncGeneratedBlockHandler)

    blockGenerator.start()
    receiverThread.get.execute {
      new Runnable {
        val buffer = ArrayBuffer[Message]()

        override def run() = {
          try {
            val consumer = consumerFactory.newConsumer(acknowledgementMode)
            while (running) {
              val message = consumer.receive()
              blockGenerator.addData(message)

            }
          } catch {
            case e: Throwable =>
              logError(e.getLocalizedMessage, e)
              restart(e.getLocalizedMessage, e)
          }
        }
      }
    }

  }

  /** Class to handle blocks generated by the block generator. */
  private final class AsyncGeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {

    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {

    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      val messages = arrayBuffer.map(_.asInstanceOf[Message])
      store(messages.flatMap(messageConverter(_)))
      messages.foreach(_.acknowledge())
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }

}

sealed trait JmsDestinationInfo {

}

case class QueueJmsDestinationInfo(queueName: String) extends JmsDestinationInfo

case class DurableTopicJmsDestinationInfo(topicName: String,
                                          subscriptionName: String) extends JmsDestinationInfo

/**
  * Implement to setup Jms consumer programmatically. Must serializable i.e. Don't put Jms object in
  * non transient fields.
  */
trait MessageConsumerFactory extends Serializable {
  @volatile
  @transient
  var connection: Connection = _

  def newConsumer(acknowledgeMode: Int): MessageConsumer = {
    stopConnection()
    connection = makeConnection
    val session = makeSession(acknowledgeMode)
    val consumer = makeConsumer(session)
    connection.start()
    consumer
  }

  private def makeSession(acknowledgeMode: Int): Session = {
    connection.createSession(false, acknowledgeMode)
  }



  def stopConnection(): Unit = {
    try {
      if (connection != null) {
        connection.close()
      }
    } finally {
      connection = null
    }
  }

  /**
    * Over ride to make new connection
    *
    * @return
    */
  def makeConnection: Connection

  /**
    * Build new consumer
    *
    * @param session
    * @return
    */
  def makeConsumer(session: Session): MessageConsumer
}

/**
  * Build Jms objects from JNDI
  *
  * @param jndiProperties        Implementation specific. JNDI properties with setup for connection
  *                              factory and destinations.
  * @param destinationInfo       Queue or Topic destination info.
  * @param connectionFactoryName Name of connection factory connfigured in JNDI
  * @param messageSelector       Message selector. Use Empty string for no message filter.
  */
case class JndiMessageConsumerFactory(jndiProperties: Properties,
                                      destinationInfo: JmsDestinationInfo,
                                      connectionFactoryName: String = "ConnectionFactory",
                                      messageSelector: String = ""
                                     )
  extends MessageConsumerFactory with Logging {

  @volatile
  @transient
  var initialContext: InitialContext = _

  override def makeConsumer(session: Session): MessageConsumer = {
    destinationInfo match {
      case DurableTopicJmsDestinationInfo(topicName, subName) =>
        val dest = initialContext.lookup(topicName).asInstanceOf[Topic]
        session.createDurableSubscriber(dest,
          subName,
          messageSelector,
          false)
      case QueueJmsDestinationInfo(qName: String) =>
        val dest = initialContext.lookup(qName).asInstanceOf[Destination]
        session.createConsumer(dest, messageSelector)
    }
  }

  override def makeConnection: Connection = {
    if (initialContext == null) {
      initialContext = new InitialContext(jndiProperties)
    }
    val connectionFactory = initialContext
      .lookup(connectionFactoryName).asInstanceOf[ConnectionFactory]

    val createConnection: Connection = connectionFactory.createConnection()
    createConnection
  }


}
