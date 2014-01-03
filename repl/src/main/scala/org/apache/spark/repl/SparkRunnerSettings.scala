package org.apache.spark.repl

import scala.tools.nsc.Settings

/**
 * <i>scala.tools.nsc.Settings</i> implementation adding Spark-specific REPL
 * command line options.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
class SparkRunnerSettings(error: String => Unit) extends Settings(error){

  val loadfiles = MultiStringSetting(
      "-i",
      "file",
      "load a file (assumes the code is given interactively)")
}
