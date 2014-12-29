package edu.berkeley.cs.amplab.sparkr

// Definitions of methods implemented by SparkRBackend
// Corresponding definition found in SparkRBackendInterface.R
// 
// NOTE: If you change any method here, you will need to change the
// corresponding one in SparkRBackendInterface.R
trait SparkRBackendInterface {

  /**
   * Create a Spark Context
   *
   * @return String the applicationId corresponding to this SparkContext
   *
   */
  def createSparkContext(master: String, appName: String, sparkHome: String,
    jars: Array[String], sparkEnvirMap: Map[String, String],
    sparkExecutorEnvMap: Map[String, String]): String

  // def saveAsTextFile(rdd: Int, path: String): Int

  // def saveAsObjectFile(rdd: Int, path: String): Int
}
