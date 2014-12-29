package edu.berkeley.cs.amplab.sparkr

object SparkRConf {

  def getSystemProperty(name: String, default: String) = {
    Option(System.getProperty(name)).getOrElse(default)
  }

  def getIntProperty(name: String, default: Int) = {
    Integer.parseInt(getSystemProperty(name, default.toString))
  }

  val numServerThreads = getIntProperty("sparkr.backend.threads", 2)
}
