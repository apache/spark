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
trait LeaderElectionAgent extends Actor {
  val masterActor: ActorRef
}

/** Single-node implementation of LeaderElectionAgent -- we're initially and always the leader. */
class MonarchyLeaderAgent(val masterActor: ActorRef) extends LeaderElectionAgent {
  override def preStart() {
    masterActor ! ElectedLeader
  }

  override def receive = {
    case _ =>
  }
}
