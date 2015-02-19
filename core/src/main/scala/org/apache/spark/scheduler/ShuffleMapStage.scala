package org.apache.spark.scheduler

import org.apache.spark.rdd.RDD
import org.apache.spark.ShuffleDependency
import org.apache.spark.util.CallSite

/**
 * Define a class that represents the ShuffleMapStage to help clean up the DAGScheduler class 
 */
private[spark] class ShuffleMapStage(override val id: Int,
                                     override val rdd: RDD[_],
                                     override val numTasks: Int,
                                     override val parents: List[Stage],
                                     override val jobId: Int,
                                     override val callSite: CallSite,
                                     val shuffleDep: ShuffleDependency[_, _, _])
  extends Stage(id, rdd, numTasks, parents, jobId, callSite) {

  override def toString = "ShuffleMapStage " + id


}
