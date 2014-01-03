package org.apache.spark.repl

import scala.tools.nsc.{Settings, CompilerCommand}
import scala.Predef._

/**
 * Command class enabling Spark-specific command line options (provided by
 * <i>org.apache.spark.repl.SparkRunnerSettings</i>).
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
class SparkCommandLine(args: List[String], override val settings: Settings)
    extends CompilerCommand(args, settings) {

  def this(args: List[String], error: String => Unit) {
    this(args, new SparkRunnerSettings(error))
  }

  def this(args: List[String]) {
    this(args, str => Console.println("Error: " + str))
  }
}
