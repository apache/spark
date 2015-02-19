package org.apache.spark.scheduler

import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * Created by zjb238 on 2/17/15.
 */
private[spark] class ResultStage(override val id: Int,
                                 override val rdd: RDD[_],
                                 override val numTasks: Int,
                                 override val parents: List[Stage],
                                 override val jobId: Int,
                                 override val callSite: CallSite)
  extends Stage(id, rdd, numTasks, parents, jobId, callSite) {

  /** For stages that are the final (consists of only ResultTasks), link to the ActiveJob. */
  var resultOfJob: Option[ActiveJob] = None

}
