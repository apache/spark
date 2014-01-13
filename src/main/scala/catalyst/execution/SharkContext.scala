package catalyst.execution

import java.io.PrintStream
import java.util.{ArrayList => JArrayList}

import scala.collection.Map
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessor
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{SparkContext, SparkEnv}


class SharkContext(
    master: String,
    jobName: String,
    sparkHome: String,
    jars: Seq[String],
    environment: Map[String, String])
  extends SparkContext(master, jobName, sparkHome, jars, environment) {

  @transient val sparkEnv = SparkEnv.get

  SharkContext.init()
  import SharkContext._

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = {
    SparkEnv.set(sparkEnv)
    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

    SessionState.start(sessionState)

    if (proc.isInstanceOf[Driver]) {
      val driver: Driver = proc.asInstanceOf[Driver]
      driver.init()

      val results = new JArrayList[String]
      val response: CommandProcessorResponse = driver.run(cmd)
      // Throw an exception if there is an error in query processing.
      if (response.getResponseCode != 0) {
        driver.destroy()
        throw new QueryExecutionException(response.getErrorMessage)
      }
      driver.setMaxRows(maxRows)
      driver.getResults(results)
      driver.destroy()
      results
    } else {
      sessionState.out.println(tokens(0) + " " + cmd_1)
      Seq(proc.run(cmd_1).getResponseCode.toString)
    }
  }
}


object SharkContext {
  // Since we can never properly shut down Hive, we put the Hive related initializations
  // here in a global singleton.

  @transient val hiveconf = new HiveConf(classOf[SessionState])

  @transient val sessionState = new SessionState(hiveconf)
  sessionState.out = new PrintStream(System.out, true, "UTF-8")
  sessionState.err = new PrintStream(System.out, true, "UTF-8")

  // A dummy init to make sure the object is properly initialized.
  def init() {}
}


