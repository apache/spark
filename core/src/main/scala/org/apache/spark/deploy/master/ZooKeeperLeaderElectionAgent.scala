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

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatchListener, LeaderLatch}

private[spark] class ZooKeeperLeaderElectionAgent(val masterActor: ActorRef,
    masterUrl: String, conf: SparkConf)
  extends LeaderElectionAgent with LeaderLatchListener with Logging  {

  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  override def preStart() {

    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)

    leaderLatch.start()
  }

  override def preRestart(reason: scala.Throwable, message: scala.Option[scala.Any]) {
    logError("LeaderElectionAgent failed...", reason)
    super.preRestart(reason, message)
  }

  override def postStop() {
    leaderLatch.close()
    zk.close()
  }

  override def receive = {
    case _ =>
  }

  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
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
