
package org.apache.spark.deploy.master

sealed trait MasterMessages extends Serializable

/** Contains messages seen only by the Master and its associated entities. */
private[master] object MasterMessages {

  // LeaderElectionAgent to Master

  case object ElectedLeader

  case object RevokedLeadership

  // Master to itself

  case object CheckForWorkerTimeOut

  case class BeginRecovery(storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo])

  case object CompleteRecovery

  case object BoundPortsRequest

  case class BoundPortsResponse(rpcEndpointPort: Int, webUIPort: Int, restPort: Option[Int])
}
