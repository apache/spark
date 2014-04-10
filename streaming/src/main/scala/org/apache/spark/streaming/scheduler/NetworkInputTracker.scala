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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable.{HashMap, SynchronizedMap, SynchronizedQueue}

import akka.actor._

import org.apache.spark.{Logging, SparkEnv, SparkException}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{NetworkReceiver, StopReceiver}
import org.apache.spark.util.AkkaUtils

/** Information about receiver */
case class ReceiverInfo(streamId: Int, typ: String, location: String) {
  override def toString = s"$typ-$streamId"
}

/** Information about blocks received by the network receiver */
case class ReceivedBlockInfo(
    streamId: Int,
    blockId: StreamBlockId,
    numRecords: Long,
    metadata: Any
  )

/**
 * Messages used by the NetworkReceiver and the NetworkInputTracker to communicate
 * with each other.
 */
private[streaming] sealed trait NetworkInputTrackerMessage
private[streaming] case class RegisterReceiver(
    streamId: Int,
    typ: String,
    host: String,
    receiverActor: ActorRef
  ) extends NetworkInputTrackerMessage
private[streaming] case class AddBlocks(receivedBlockInfo: ReceivedBlockInfo)
  extends NetworkInputTrackerMessage
private[streaming] case class DeregisterReceiver(streamId: Int, msg: String)
  extends NetworkInputTrackerMessage

/**
 * This class manages the execution of the receivers of NetworkInputDStreams. Instance of
 * this class must be created after all input streams have been added and StreamingContext.start()
 * has been called because it needs the final set of input streams at the time of instantiation.
 */
private[streaming]
class NetworkInputTracker(ssc: StreamingContext) extends Logging {

  val networkInputStreams = ssc.graph.getNetworkInputStreams()
  val networkInputStreamMap = Map(networkInputStreams.map(x => (x.id, x)): _*)
  val receiverExecutor = new ReceiverExecutor()
  val receiverInfo = new HashMap[Int, ActorRef] with SynchronizedMap[Int, ActorRef]
  val receivedBlockInfo = new HashMap[Int, SynchronizedQueue[ReceivedBlockInfo]]
    with SynchronizedMap[Int, SynchronizedQueue[ReceivedBlockInfo]]
  val timeout = AkkaUtils.askTimeout(ssc.conf)
  val listenerBus = ssc.scheduler.listenerBus

  // actor is created when generator starts.
  // This not being null means the tracker has been started and not stopped
  var actor: ActorRef = null
  var currentTime: Time = null

  /** Start the actor and receiver execution thread. */
  def start() = synchronized {
    if (actor != null) {
      throw new SparkException("NetworkInputTracker already started")
    }

    if (!networkInputStreams.isEmpty) {
      actor = ssc.env.actorSystem.actorOf(Props(new NetworkInputTrackerActor),
        "NetworkInputTracker")
      receiverExecutor.start()
      logInfo("NetworkInputTracker started")
    }
  }

  /** Stop the receiver execution thread. */
  def stop() = synchronized {
    if (!networkInputStreams.isEmpty && actor != null) {
      // First, stop the receivers
      receiverExecutor.stop()

      // Finally, stop the actor
      ssc.env.actorSystem.stop(actor)
      actor = null
      logInfo("NetworkInputTracker stopped")
    }
  }

  /** Return all the blocks received from a receiver. */
  def getReceivedBlockInfo(streamId: Int): Array[ReceivedBlockInfo] = {
    val receivedBlockInfo = getReceivedBlockInfoQueue(streamId).dequeueAll(x => true)
    logInfo("Stream " + streamId + " received " + receivedBlockInfo.size + " blocks")
    receivedBlockInfo.toArray
  }

  private def getReceivedBlockInfoQueue(streamId: Int) = {
    receivedBlockInfo.getOrElseUpdate(streamId, new SynchronizedQueue[ReceivedBlockInfo])
  }

  /** Register a receiver */
  def registerReceiver(streamId: Int, typ: String, host: String, receiverActor: ActorRef, sender: ActorRef) {
    if (!networkInputStreamMap.contains(streamId)) {
      throw new Exception("Register received for unexpected id " + streamId)
    }
    receiverInfo += ((streamId, receiverActor))
    ssc.scheduler.listenerBus.post(StreamingListenerReceiverStarted(
      ReceiverInfo(streamId, typ, host)
    ))
    logInfo("Registered receiver for network stream " + streamId + " from " + sender.path.address)
  }

  /** Deregister a receiver */
  def deregisterReceiver(streamId: Int, message: String) {
    receiverInfo -= streamId
    logError("Deregistered receiver for network stream " + streamId + " with message:\n" + message)
  }

  /** Add new blocks for the given stream */
  def addBlocks(receivedBlockInfo: ReceivedBlockInfo) {
    getReceivedBlockInfoQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    logDebug("Stream " + receivedBlockInfo.streamId + " received new blocks: " +
      receivedBlockInfo.blockId)
  }

  /** Check if any blocks are left to be processed */
  def hasMoreReceivedBlockIds: Boolean = {
    !receivedBlockInfo.values.forall(_.isEmpty)
  }

  /** Actor to receive messages from the receivers. */
  private class NetworkInputTrackerActor extends Actor {
    def receive = {
      case RegisterReceiver(streamId, typ, host, receiverActor) =>
        registerReceiver(streamId, typ, host, receiverActor, sender)
        sender ! true
      case AddBlocks(receivedBlockInfo) =>
        addBlocks(receivedBlockInfo)
      case DeregisterReceiver(streamId, message) =>
        deregisterReceiver(streamId, message)
        sender ! true
    }
  }

  /** This thread class runs all the receivers on the cluster.  */
  class ReceiverExecutor {
    @transient val env = ssc.env
    @transient val thread  = new Thread() {
      override def run() {
        try {
          SparkEnv.set(env)
          startReceivers()
        } catch {
          case ie: InterruptedException => logInfo("ReceiverExecutor interrupted")
        }
      }
    }

    def start() {
      thread.start()
    }

    def stop() {
      // Send the stop signal to all the receivers
      stopReceivers()

      // Wait for the Spark job that runs the receivers to be over
      // That is, for the receivers to quit gracefully.
      thread.join(10000)

      // Check if all the receivers have been deregistered or not
      if (!receiverInfo.isEmpty) {
        logWarning("All of the receivers have not deregistered, " + receiverInfo)
      } else {
        logInfo("All of the receivers have deregistered successfully")
      }
    }

    /**
     * Get the receivers from the NetworkInputDStreams, distributes them to the
     * worker nodes as a parallel collection, and runs them.
     */
    private def startReceivers() {
      val receivers = networkInputStreams.map(nis => {
        val rcvr = nis.getReceiver()
        rcvr.setStreamId(nis.id)
        rcvr
      })

      // Right now, we only honor preferences if all receivers have them
      val hasLocationPreferences = receivers.map(_.getLocationPreference().isDefined)
        .reduce(_ && _)

      // Create the parallel collection of receivers to distributed them on the worker nodes
      val tempRDD =
        if (hasLocationPreferences) {
          val receiversWithPreferences =
            receivers.map(r => (r, Seq(r.getLocationPreference().toString)))
          ssc.sc.makeRDD[NetworkReceiver[_]](receiversWithPreferences)
        }
        else {
          ssc.sc.makeRDD(receivers, receivers.size)
        }

      // Function to start the receiver on the worker node
      val startReceiver = (iterator: Iterator[NetworkReceiver[_]]) => {
        if (!iterator.hasNext) {
          throw new Exception("Could not start receiver as details not found.")
        }
        iterator.next().start()
      }
      // Run the dummy Spark job to ensure that all slaves have registered.
      // This avoids all the receivers to be scheduled on the same node.
      if (!ssc.sparkContext.isLocal) {
        ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
      }

      // Distribute the receivers and start them
      logInfo("Starting " + receivers.length + " receivers")
      ssc.sparkContext.runJob(tempRDD, startReceiver)
      logInfo("All of the receivers have been terminated")
    }

    /** Stops the receivers. */
    private def stopReceivers() {
      // Signal the receivers to stop
      receiverInfo.values.foreach(_ ! StopReceiver)
      logInfo("Sent stop signal to all " + receiverInfo.size + " receivers")
    }
  }
}
