
package org.apache.spark.deploy.master

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * A LeaderElectionAgent tracks current master and is a common interface for all election Agents.
 */
@DeveloperApi
trait LeaderElectionAgent {
  val masterInstance: LeaderElectable
  def stop() {} // to avoid noops in implementations.
}

@DeveloperApi
trait LeaderElectable {
  def electedLeader(): Unit
  def revokedLeadership(): Unit
}

/** Single-node implementation of LeaderElectionAgent -- we're initially and always the leader. */
private[spark] class MonarchyLeaderAgent(val masterInstance: LeaderElectable)
  extends LeaderElectionAgent {
  masterInstance.electedLeader()
}
