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

package org.apache.spark.streaming.receivers

import akka.actor.{ Actor, PoisonPill, Props, SupervisorStrategy }
import akka.actor.{ actorRef2Scala, ActorRef }
import akka.actor.{ PossiblyHarmful, OneForOneStrategy }
import akka.actor.SupervisorStrategy._

import scala.concurrent.duration._
import scala.reflect.ClassTag

import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.dstream.NetworkReceiver

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

/** A helper with set of defaults for supervisor strategy */
object ReceiverSupervisorStrategy {

  val defaultStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange =
    15 millis) {
    case _: RuntimeException => Restart
    case _: Exception => Escalate
  }
}

/**
 * A receiver trait to be mixed in with your Actor to gain access to
 * the API for pushing received data into Spark Streaming for being processed.
 *
 * Find more details at: http://spark-project.org/docs/latest/streaming-custom-receivers.html
 * 
 * @example {{{
 *  class MyActor extends Actor with Receiver{
 *      def receive {
 *          case anything: String => pushBlock(anything)
 *      }
 *  }
 *
 *  // Can be used with an actorStream as follows
 *  ssc.actorStream[String](Props(new MyActor),"MyActorReceiver")
 *
 * }}}
 *
 * @note Since Actor may exist outside the spark framework, It is thus user's responsibility
 *       to ensure the type safety, i.e parametrized type of push block and InputDStream
 *       should be same.
 */
trait Receiver {

  self: Actor => // to ensure that this can be added to Actor classes only

  /**
   * Push an iterator received data into Spark Streaming for processing
   */
  def pushBlock[T: ClassTag](iter: Iterator[T]) {
    context.parent ! Data(iter)
  }

  /**
   * Push a single item of received data into Spark Streaming for processing
   */
  def pushBlock[T: ClassTag](data: T) {
    context.parent ! Data(data)
  }
}

/**
 * Statistics for querying the supervisor about state of workers. Used in
 * conjunction with `StreamingContext.actorStream` and
 * [[org.apache.spark.streaming.receivers.Receiver]].
 */
case class Statistics(numberOfMsgs: Int,
  numberOfWorkers: Int,
  numberOfHiccups: Int,
  otherInfo: String)

/** Case class to receive data sent by child actors */
private[streaming] case class Data[T: ClassTag](data: T)

/**
 * Provides Actors as receivers for receiving stream.
 *
 * As Actors can also be used to receive data from almost any stream source.
 * A nice set of abstraction(s) for actors as receivers is already provided for
 * a few general cases. It is thus exposed as an API where user may come with
 * his own Actor to run as receiver for Spark Streaming input source.
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
private[streaming] class ActorReceiver[T: ClassTag](
  props: Props,
  name: String,
  storageLevel: StorageLevel,
  receiverSupervisorStrategy: SupervisorStrategy)
  extends NetworkReceiver[T] {

  protected lazy val blocksGenerator: BlockGenerator =
    new BlockGenerator(storageLevel)

  protected lazy val supervisor = env.actorSystem.actorOf(Props(new Supervisor),
    "Supervisor" + streamId)

  class Supervisor extends Actor {

    override val supervisorStrategy = receiverSupervisorStrategy
    val worker = context.actorOf(props, name)
    logInfo("Started receiver worker at:" + worker.path)

    val n: AtomicInteger = new AtomicInteger(0)
    val hiccups: AtomicInteger = new AtomicInteger(0)

    def receive = {

      case Data(iter: Iterator[_]) => pushBlock(iter.asInstanceOf[Iterator[T]])

      case Data(msg) =>
        blocksGenerator += msg.asInstanceOf[T]
        n.incrementAndGet

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

  protected def pushBlock(iter: Iterator[T]) {
    val buffer = new ArrayBuffer[T]
    buffer ++= iter
    pushBlock(StreamBlockId(streamId, System.nanoTime()), buffer, null, storageLevel)
  }

  protected def onStart() = {
    blocksGenerator.start()
    supervisor
    logInfo("Supervision tree for receivers initialized at:" + supervisor.path)
  }

  protected def onStop() = {
    supervisor ! PoisonPill
  }

}
