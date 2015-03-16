package edu.berkeley.cs.amplab.sparkr

/**
 * Configuration options for SparkRBackend server
 */
object SparkRConf {

  def getSystemProperty(name: String, default: String) = {
    Option(System.getProperty(name)).getOrElse(default)
  }

  def getIntProperty(name: String, default: Int) = {
    Integer.parseInt(getSystemProperty(name, default.toString))
  }

  // Number of threads to use in the Netty server
  val numServerThreads = getIntProperty("sparkr.backend.threads", 2)
}
