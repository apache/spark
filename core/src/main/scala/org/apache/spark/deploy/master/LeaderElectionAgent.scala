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

import akka.actor.{Actor, ActorRef}

import org.apache.spark.deploy.master.MasterMessages.ElectedLeader

/**
 * A LeaderElectionAgent keeps track of whether the current Master is the leader, meaning it
 * is the only Master serving requests.
 * In addition to the API provided, the LeaderElectionAgent will use of the following messages
 * to inform the Master of leader changes:
 * [[org.apache.spark.deploy.master.MasterMessages.ElectedLeader ElectedLeader]]
 * [[org.apache.spark.deploy.master.MasterMessages.RevokedLeadership RevokedLeadership]]
 */
private[spark] trait LeaderElectionAgent extends Actor {
  val masterActor: ActorRef
}

/** Single-node implementation of LeaderElectionAgent -- we're initially and always the leader. */
private[spark] class MonarchyLeaderAgent(val masterActor: ActorRef) extends LeaderElectionAgent {
  override def preStart() {
    masterActor ! ElectedLeader
  }

  override def receive = {
    case _ =>
  }
}
