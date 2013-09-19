package org.apache.spark.deploy.master

import org.apache.spark.util.Utils

sealed trait MasterMessages extends Serializable

/** Contains messages seen only by the Master and its associated entities. */
private[master] object MasterMessages {

  // LeaderElectionAgent to Master

  case object ElectedLeader

  case object RevokedLeadership

  // Actor System to LeaderElectionAgent

  case object CheckLeader

  // Actor System to Master

  case object CheckForWorkerTimeOut

  case class BeginRecovery(storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo])

  case object RequestWebUIPort

  case class WebUIPortResponse(webUIBoundPort: Int)
}
