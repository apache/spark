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

import org.apache.spark.streaming.dstream.{NetworkInputDStream, NetworkReceiver}
import org.apache.spark.streaming.dstream.{StopReceiver, ReportBlock, ReportError}
import org.apache.spark.{SparkException, Logging, SparkEnv}
import org.apache.spark.SparkContext._

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.dispatch._
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.util.AkkaUtils

private[streaming] sealed trait NetworkInputTrackerMessage
private[streaming] case class RegisterReceiver(streamId: Int, receiverActor: ActorRef)
  extends NetworkInputTrackerMessage
private[streaming] case class AddBlocks(streamId: Int, blockIds: Seq[BlockId], metadata: Any)
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
  val receiverInfo = new HashMap[Int, ActorRef]
  val receivedBlockIds = new HashMap[Int, Queue[BlockId]]
  val timeout = AkkaUtils.askTimeout(ssc.conf)


  // actor is created when generator starts.
  // This not being null means the tracker has been started and not stopped
  var actor: ActorRef = null
  var currentTime: Time = null

  /** Start the actor and receiver execution thread. */
  def start() {
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
  def stop() {
    if (!networkInputStreams.isEmpty && actor != null) {
      receiverExecutor.interrupt()
      receiverExecutor.stopReceivers()
      ssc.env.actorSystem.stop(actor)
      logInfo("NetworkInputTracker stopped")
    }
  }

  /** Return all the blocks received from a receiver. */
  def getBlockIds(receiverId: Int, time: Time): Array[BlockId] = synchronized {
    val queue =  receivedBlockIds.synchronized {
      receivedBlockIds.getOrElse(receiverId, new Queue[BlockId]())
    }
    val result = queue.synchronized {
      queue.dequeueAll(x => true)
    }
    logInfo("Stream " + receiverId + " received " + result.size + " blocks")
    result.toArray
  }

  /** Actor to receive messages from the receivers. */
  private class NetworkInputTrackerActor extends Actor {
    def receive = {
      case RegisterReceiver(streamId, receiverActor) => {
        if (!networkInputStreamMap.contains(streamId)) {
          throw new Exception("Register received for unexpected id " + streamId)
        }
        receiverInfo += ((streamId, receiverActor))
        logInfo("Registered receiver for network stream " + streamId + " from "
          + sender.path.address)
        sender ! true
      }
      case AddBlocks(streamId, blockIds, metadata) => {
        val tmp = receivedBlockIds.synchronized {
          if (!receivedBlockIds.contains(streamId)) {
            receivedBlockIds += ((streamId, new Queue[BlockId]))
          }
          receivedBlockIds(streamId)
        }
        tmp.synchronized {
          tmp ++= blockIds
        }
        networkInputStreamMap(streamId).addMetadata(metadata)
      }
      case DeregisterReceiver(streamId, msg) => {
        receiverInfo -= streamId
        logError("De-registered receiver for network stream " + streamId
          + " with message " + msg)
        //TODO: Do something about the corresponding NetworkInputDStream
      }
    }
  }

  /** This thread class runs all the receivers on the cluster.  */
  class ReceiverExecutor extends Thread {
    val env = ssc.env

    override def run() {
      try {
        SparkEnv.set(env)
        startReceivers()
      } catch {
        case ie: InterruptedException => logInfo("ReceiverExecutor interrupted")
      } finally {
        stopReceivers()
      }
    }

    /**
     * Get the receivers from the NetworkInputDStreams, distributes them to the
     * worker nodes as a parallel collection, and runs them.
     */
    def startReceivers() {
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
      ssc.sparkContext.runJob(tempRDD, startReceiver)
    }

    /** Stops the receivers. */
    def stopReceivers() {
      // Signal the receivers to stop
      receiverInfo.values.foreach(_ ! StopReceiver)
    }
  }
}
