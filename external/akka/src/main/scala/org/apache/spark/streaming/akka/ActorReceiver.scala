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

package org.apache.spark.streaming.akka

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * :: DeveloperApi ::
 * A helper with set of defaults for supervisor strategy
 */
@DeveloperApi
object ActorReceiver {

  /**
   * A OneForOneStrategy supervisor strategy with `maxNrOfRetries = 10` and
   * `withinTimeRange = 15 millis`. For RuntimeException, it will restart the ActorReceiver; for
   * others, it just escalates the failure to the supervisor of the supervisor.
   */
  val defaultSupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange =
    15 millis) {
    case _: RuntimeException => Restart
    case _: Exception => Escalate
  }

  /**
   * A default ActorSystem creator. It will use a unique system name
   * (streaming-actor-system-<spark-task-attempt-id>) to start an ActorSystem that supports remote
   * communication.
   */
  val defaultActorSystemCreator: () => ActorSystem = () => {
    val uniqueSystemName = s"streaming-actor-system-${TaskContext.get().taskAttemptId()}"
    val akkaConf = ConfigFactory.parseString(
      s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
         |""".stripMargin)
    ActorSystem(uniqueSystemName, akkaConf)
  }
}

/**
 * :: DeveloperApi ::
 * A base Actor that provides APIs for pushing received data into Spark Streaming for processing.
 *
 * Find more details at: http://spark.apache.org/docs/latest/streaming-custom-receivers.html
 *
 * @example {{{
 *  class MyActor extends ActorReceiver {
 *      def receive {
 *          case anything: String => store(anything)
 *      }
 *  }
 *
 *  AkkaUtils.createStream[String](ssc, Props[MyActor](),"MyActorReceiver")
 *
 * }}}
 *
 * @note Since Actor may exist outside the spark framework, It is thus user's responsibility
 *       to ensure the type safety, i.e. parametrized type of push block and InputDStream
 *       should be same.
 */
@DeveloperApi
abstract class ActorReceiver extends Actor {

  /** Store an iterator of received data as a data block into Spark's memory. */
  def store[T](iter: Iterator[T]) {
    context.parent ! IteratorData(iter)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory. Note
   * that the data in the ByteBuffer must be serialized using the same serializer
   * that Spark is configured to use.
   */
  def store(bytes: ByteBuffer) {
    context.parent ! ByteBufferData(bytes)
  }

  /**
   * Store a single item of received data to Spark's memory asynchronously.
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
   */
  def store[T](item: T) {
    context.parent ! SingleItemData(item)
  }

  /**
   * Store a single item of received data to Spark's memory and returns a `Future`.
   * The `Future` will be completed when the operator finishes, or with an
   * `akka.pattern.AskTimeoutException` after the given timeout has expired.
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
   *
   * This method allows the user to control the flow speed using `Future`
   */
  def store[T](item: T, timeout: Timeout): Future[Unit] = {
    context.parent.ask(AskStoreSingleItemData(item))(timeout).map(_ => ())(context.dispatcher)
  }
}

/**
 * :: DeveloperApi ::
 * A Java UntypedActor that provides APIs for pushing received data into Spark Streaming for
 * processing.
 *
 * Find more details at: http://spark.apache.org/docs/latest/streaming-custom-receivers.html
 *
 * @example {{{
 *  class MyActor extends JavaActorReceiver {
 *      @Override
 *      public void onReceive(Object msg) throws Exception {
 *          store((String) msg);
 *      }
 *  }
 *
 *  AkkaUtils.<String>createStream(jssc, Props.create(MyActor.class), "MyActorReceiver");
 *
 * }}}
 *
 * @note Since Actor may exist outside the spark framework, It is thus user's responsibility
 *       to ensure the type safety, i.e. parametrized type of push block and InputDStream
 *       should be same.
 */
@DeveloperApi
abstract class JavaActorReceiver extends UntypedActor {

  /** Store an iterator of received data as a data block into Spark's memory. */
  def store[T](iter: Iterator[T]) {
    context.parent ! IteratorData(iter)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory. Note
   * that the data in the ByteBuffer must be serialized using the same serializer
   * that Spark is configured to use.
   */
  def store(bytes: ByteBuffer) {
    context.parent ! ByteBufferData(bytes)
  }

  /**
   * Store a single item of received data to Spark's memory.
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
   */
  def store[T](item: T) {
    context.parent ! SingleItemData(item)
  }

  /**
   * Store a single item of received data to Spark's memory and returns a `Future`.
   * The `Future` will be completed when the operator finishes, or with an
   * `akka.pattern.AskTimeoutException` after the given timeout has expired.
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
   *
   * This method allows the user to control the flow speed using `Future`
   */
  def store[T](item: T, timeout: Timeout): Future[Unit] = {
    context.parent.ask(AskStoreSingleItemData(item))(timeout).map(_ => ())(context.dispatcher)
  }
}

/**
 * :: DeveloperApi ::
 * Statistics for querying the supervisor about state of workers. Used in
 * conjunction with `AkkaUtils.createStream` and
 * [[org.apache.spark.streaming.akka.ActorReceiverSupervisor]].
 */
@DeveloperApi
case class Statistics(numberOfMsgs: Int,
  numberOfWorkers: Int,
  numberOfHiccups: Int,
  otherInfo: String)

/** Case class to receive data sent by child actors */
private[akka] sealed trait ActorReceiverData
private[akka] case class SingleItemData[T](item: T) extends ActorReceiverData
private[akka] case class AskStoreSingleItemData[T](item: T) extends ActorReceiverData
private[akka] case class IteratorData[T](iterator: Iterator[T]) extends ActorReceiverData
private[akka] case class ByteBufferData(bytes: ByteBuffer) extends ActorReceiverData
private[akka] object Ack extends ActorReceiverData

/**
 * Provides Actors as receivers for receiving stream.
 *
 * As Actors can also be used to receive data from almost any stream source.
 * A nice set of abstraction(s) for actors as receivers is already provided for
 * a few general cases. It is thus exposed as an API where user may come with
 * their own Actor to run as receiver for Spark Streaming input source.
 *
 * This starts a supervisor actor which starts workers and also provides
 * [http://doc.akka.io/docs/akka/snapshot/scala/fault-tolerance.html fault-tolerance].
 *
 * Here's a way to start more supervisor/workers as its children.
 *
 * @example {{{
 *  context.parent ! Props(new Supervisor)
 * }}} OR {{{
 *  context.parent ! Props(new Worker, "Worker")
 * }}}
 */
private[akka] class ActorReceiverSupervisor[T: ClassTag](
    actorSystemCreator: () => ActorSystem,
    props: Props,
    name: String,
    storageLevel: StorageLevel,
    receiverSupervisorStrategy: SupervisorStrategy
  ) extends Receiver[T](storageLevel) with Logging {

  private lazy val actorSystem = actorSystemCreator()
  protected lazy val actorSupervisor = actorSystem.actorOf(Props(new Supervisor),
    "Supervisor" + streamId)

  class Supervisor extends Actor {

    override val supervisorStrategy = receiverSupervisorStrategy
    private val worker = context.actorOf(props, name)
    logInfo("Started receiver worker at:" + worker.path)

    private val n: AtomicInteger = new AtomicInteger(0)
    private val hiccups: AtomicInteger = new AtomicInteger(0)

    override def receive: PartialFunction[Any, Unit] = {

      case IteratorData(iterator) =>
        logDebug("received iterator")
        store(iterator.asInstanceOf[Iterator[T]])

      case SingleItemData(msg) =>
        logDebug("received single")
        store(msg.asInstanceOf[T])
        n.incrementAndGet

      case AskStoreSingleItemData(msg) =>
        logDebug("received single sync")
        store(msg.asInstanceOf[T])
        n.incrementAndGet
        sender() ! Ack

      case ByteBufferData(bytes) =>
        logDebug("received bytes")
        store(bytes)

      case props: Props =>
        val worker = context.actorOf(props)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case (props: Props, name: String) =>
        val worker = context.actorOf(props, name)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case _: PossiblyHarmful => hiccups.incrementAndGet()

      case _: Statistics =>
        val workers = context.children
        sender ! Statistics(n.get, workers.size, hiccups.get, workers.mkString("\n"))

    }
  }

  def onStart(): Unit = {
    actorSupervisor
    logInfo("Supervision tree for receivers initialized at:" + actorSupervisor.path)
  }

  def onStop(): Unit = {
    actorSupervisor ! PoisonPill
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }
}
