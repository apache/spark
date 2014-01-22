package catalyst.execution

import java.io._
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
  @transient val hiveconf = new HiveConf(classOf[SessionState])

  @transient val sessionState = new SessionState(hiveconf)

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  val outputBuffer =  new java.io.OutputStream {
    var pos: Int = 0
    var buffer = new Array[Int](1024)
    def write(i: Int): Unit = {
      buffer(pos) = i
      pos = (pos + 1) % buffer.size
    }

    override def toString = {
      val (end, start) = buffer.splitAt(pos)
      val input = new java.io.InputStream {
        val iterator = (start ++ end).iterator

        def read(): Int = if(iterator.hasNext) iterator.next else -1
      }
      val reader = new BufferedReader(new InputStreamReader(input))
      val stringBuilder = new StringBuilder
      var line = reader.readLine()
      while(line != null) {
        stringBuilder.append(line)
        line = reader.readLine()
      }
      stringBuilder.toString()
    }
  }

  sessionState.err = new PrintStream(outputBuffer, true, "UTF-8")
  sessionState.out = new PrintStream(outputBuffer, true, "UTF-8")

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = {
    try {
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
    }catch {
      case e: Exception =>
        println(
          """
            |======================
            |HIVE FAILURE OUTPUT
            |======================
          """.stripMargin)
        println(outputBuffer.toString)
        println(
          """
            |======================
            |END HIVE FAILURE OUTPUT
            |======================
          """.stripMargin)
        throw e
    }
  }
}


