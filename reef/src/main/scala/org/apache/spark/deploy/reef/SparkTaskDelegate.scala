package org.apache.spark.deploy.reef

/**
 * Spark-REEF API
 * SparkTaskDelegate executes Spark Executor
 */
final class SparkTaskDelegate {

  def run(args: Array[String]) {
    //simply run CoarseGrainedExecutorBackend inside REEF Task
    org.apache.spark.executor.CoarseGrainedExecutorBackend.main(args)
  }

}