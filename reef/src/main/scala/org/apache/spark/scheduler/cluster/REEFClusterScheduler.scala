package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.deploy.reef.REEFDriverDelegate

/**
 *
 * This is a simple extension to ClusterScheduler - to ensure that appropriate initialization of Evaluators
 */
private[spark] class REEFClusterScheduler(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  logInfo("Created REEFClusterScheduler")

  override def postStartHook() {
    val sparkContextInitialized = REEFDriverDelegate.sparkContextInitialized(sc)
    if (sparkContextInitialized){
      REEFDriverDelegate.acquiredSparkContext() //notify REEFDriver that SparkContext is alive
      REEFDriverDelegate.waitForREEFEvaluatorInit() //wait for REEF Evaluator to init
    }
    logInfo("REEFClusterScheduler.postStartHook done")
  }
}
