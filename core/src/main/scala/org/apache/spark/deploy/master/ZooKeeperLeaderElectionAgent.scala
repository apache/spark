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

package org.apache.spark.deploy.master

import akka.actor.ActorRef
import org.apache.zookeeper._
import org.apache.zookeeper.Watcher.Event.EventType

import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.Logging

private[spark] class ZooKeeperLeaderElectionAgent(val masterActor: ActorRef, masterUrl: String)
  extends LeaderElectionAgent with SparkZooKeeperWatcher with Logging  {

  val WORKING_DIR = System.getProperty("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  private val watcher = new ZooKeeperWatcher()
  private val zk = new SparkZooKeeperSession(this)
  private var status = LeadershipStatus.NOT_LEADER
  private var myLeaderFile: String = _
  private var leaderUrl: String = _

  override def preStart() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk.connect()
  }

  override def zkSessionCreated() {
    synchronized {
      zk.mkdirRecursive(WORKING_DIR)
      myLeaderFile =
        zk.create(WORKING_DIR + "/master_", masterUrl.getBytes, CreateMode.EPHEMERAL_SEQUENTIAL)
      self ! CheckLeader
    }
  }

  override def preRestart(reason: scala.Throwable, message: scala.Option[scala.Any]) {
    logError("LeaderElectionAgent failed, waiting " + zk.ZK_TIMEOUT_MILLIS + "...", reason)
    Thread.sleep(zk.ZK_TIMEOUT_MILLIS)
    super.preRestart(reason, message)
  }

  override def zkDown() {
    logError("ZooKeeper down! LeaderElectionAgent shutting down Master.")
    System.exit(1)
  }

  override def postStop() {
    zk.close()
  }

  override def receive = {
    case CheckLeader => checkLeader()
  }

  private class ZooKeeperWatcher extends Watcher {
    def process(event: WatchedEvent) {
      if (event.getType == EventType.NodeDeleted) {
        logInfo("Leader file disappeared, a master is down!")
        self ! CheckLeader
      }
    }
  }

  /** Uses ZK leader election. Navigates several ZK potholes along the way. */
  def checkLeader() {
    val masters = zk.getChildren(WORKING_DIR).toList
    val leader = masters.sorted.head
    val leaderFile = WORKING_DIR + "/" + leader

    // Setup a watch for the current leader.
    zk.exists(leaderFile, watcher)

    try {
      leaderUrl = new String(zk.getData(leaderFile))
    } catch {
      // A NoNodeException may be thrown if old leader died since the start of this method call.
      // This is fine -- just check again, since we're guaranteed to see the new values.
      case e: KeeperException.NoNodeException =>
        logInfo("Leader disappeared while reading it -- finding next leader")
        checkLeader()
        return
    }

    // Synchronization used to ensure no interleaving between the creation of a new session and the
    // checking of a leader, which could cause us to delete our real leader file erroneously.
    synchronized {
      val isLeader = myLeaderFile == leaderFile
      if (!isLeader && leaderUrl == masterUrl) {
        // We found a different master file pointing to this process.
        // This can happen in the following two cases:
        // (1) The master process was restarted on the same node.
        // (2) The ZK server died between creating the node and returning the name of the node.
        //     For this case, we will end up creating a second file, and MUST explicitly delete the
        //     first one, since our ZK session is still open.
        // Note that this deletion will cause a NodeDeleted event to be fired so we check again for
        // leader changes.
        assert(leaderFile < myLeaderFile)
        logWarning("Cleaning up old ZK master election file that points to this master.")
        zk.delete(leaderFile)
      } else {
        updateLeadershipStatus(isLeader)
      }
    }
  }

  def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      masterActor ! ElectedLeader
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      masterActor ! RevokedLeadership
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
