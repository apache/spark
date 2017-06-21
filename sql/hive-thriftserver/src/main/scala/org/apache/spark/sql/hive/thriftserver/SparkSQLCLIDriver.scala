/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.io._
import java.util.{ArrayList => JArrayList, Locale}

import scala.collection.JavaConverters._

import jline.console.ConsoleReader
import jline.console.history.FileHistory
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.{HiveInterruptCallback, HiveInterruptUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.log4j.{Level, Logger}
import org.apache.thrift.transport.TSocket

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.util.ShutdownHookManager

/**
 * This code doesn't support remote connections in Hive 1.2+, as the underlying CliDriver
 * has dropped its support.
 */
private[hive] object SparkSQLCLIDriver extends Logging {
  private val prompt = "spark-sql"
  private val continuedPrompt = "".padTo(prompt.length, ' ')
  private var transport: TSocket = _

  installSignalHandler()

  /**
   * Install an interrupt callback to cancel all Spark jobs. In Hive's CliDriver#processLine(),
   * a signal handler will invoke this registered callback if a Ctrl+C signal is detected while
   * a command is being processed by the current thread.
   */
  def installSignalHandler() {
    HiveInterruptUtils.add(new HiveInterruptCallback {
      override def interrupt() {
        // Handle remote execution mode
        if (SparkSQLEnv.sparkContext != null) {
          SparkSQLEnv.sparkContext.cancelAllJobs()
        } else {
          if (transport != null) {
            // Force closing of TCP connection upon session termination
            transport.getSocket.close()
          }
        }
      }
    })
  }

  def main(args: Array[String]) {
    val oproc = new OptionsProcessor()
    if (!oproc.process_stage1(args)) {
      System.exit(1)
    }

    val cliConf = new HiveConf(classOf[SessionState])
    // Override the location of the metastore since this is only used for local execution.
    HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false).foreach {
      case (key, value) => cliConf.set(key, value)
    }
    val sessionState = new CliSessionState(cliConf)

    sessionState.in = System.in
    try {
      sessionState.out = new PrintStream(System.out, true, "UTF-8")
      sessionState.info = new PrintStream(System.err, true, "UTF-8")
      sessionState.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(sessionState)) {
      System.exit(2)
    }

    // Set all properties specified via command line.
    val conf: HiveConf = sessionState.getConf
    sessionState.cmdProperties.entrySet().asScala.foreach { item =>
      val key = item.getKey.toString
      val value = item.getValue.toString
      // We do not propagate metastore options to the execution copy of hive.
      if (key != "javax.jdo.option.ConnectionURL") {
        conf.set(key, value)
        sessionState.getOverriddenConfigurations.put(key, value)
      }
    }

    SessionState.start(sessionState)

    // Clean up after we exit
    ShutdownHookManager.addShutdownHook { () => SparkSQLEnv.stop() }

    val remoteMode = isRemoteMode(sessionState)
    // "-h" option has been passed, so connect to Hive thrift server.
    if (!remoteMode) {
      // Hadoop-20 and above - we need to augment classpath using hiveconf
      // components.
      // See also: code in ExecDriver.java
      var loader = conf.getClassLoader
      val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","))
      }
      conf.setClassLoader(loader)
      Thread.currentThread().setContextClassLoader(loader)
    } else {
      // Hive 1.2 + not supported in CLI
      throw new RuntimeException("Remote operations not supported")
    }

    val cli = new SparkSQLCLIDriver
    cli.setHiveVariables(oproc.getHiveVariables)

    // TODO work around for set the log output to console, because the HiveContext
    // will set the output into an invalid buffer.
    sessionState.in = System.in
    try {
      sessionState.out = new PrintStream(System.out, true, "UTF-8")
      sessionState.info = new PrintStream(System.err, true, "UTF-8")
      sessionState.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (sessionState.database != null) {
      SparkSQLEnv.sqlContext.sessionState.catalog.setCurrentDatabase(
        s"${sessionState.database}")
    }

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(sessionState)

    // Respect the configurations set by --hiveconf from the command line
    // (based on Hive's CliDriver).
    val it = sessionState.getOverriddenConfigurations.entrySet().iterator()
    while (it.hasNext) {
      val kv = it.next()
      SparkSQLEnv.sqlContext.setConf(kv.getKey, kv.getValue)
    }

    if (sessionState.execString != null) {
      System.exit(cli.processLine(sessionState.execString))
    }

    try {
      if (sessionState.fileName != null) {
        System.exit(cli.processFile(sessionState.fileName))
      }
    } catch {
      case e: FileNotFoundException =>
        logError(s"Could not open input file for reading. (${e.getMessage})")
        System.exit(3)
    }

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    reader.setExpandEvents(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    CliDriver.getCommandCompleter.foreach((e) => reader.addCompleter(e))

    val historyDirectory = System.getProperty("user.home")

    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + ".hivehistory"
        reader.setHistory(new FileHistory(new File(historyFile)))
      } else {
        logWarning("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        logWarning("WARNING: Encountered an error while trying to initialize Hive's " +
                           "history file.  History will not be available during this session.")
        logWarning(e.getMessage)
    }

    // add shutdown hook to flush the history to history file
    ShutdownHookManager.addShutdownHook { () =>
      reader.getHistory match {
        case h: FileHistory =>
          try {
            h.flush()
          } catch {
            case e: IOException =>
              logWarning("WARNING: Failed to write command history file: " + e.getMessage)
          }
        case _ =>
      }
    }

    // TODO: missing
/*
    val clientTransportTSocketField = classOf[CliSessionState].getDeclaredField("transport")
    clientTransportTSocketField.setAccessible(true)

    transport = clientTransportTSocketField.get(sessionState).asInstanceOf[TSocket]
*/
    transport = null

    var ret = 0
    var prefix = ""
    val currentDB = ReflectionUtils.invokeStatic(classOf[CliDriver], "getFormattedDb",
      classOf[HiveConf] -> conf, classOf[CliSessionState] -> sessionState)

    def promptWithCurrentDB: String = s"$prompt$currentDB"
    def continuedPromptWithDBSpaces: String = continuedPrompt + ReflectionUtils.invokeStatic(
      classOf[CliDriver], "spacesForString", classOf[String] -> currentDB)

    var currentPrompt = promptWithCurrentDB
    var line = reader.readLine(currentPrompt + "> ")

    while (line != null) {
      if (!line.startsWith("--")) {
        if (prefix.nonEmpty) {
          prefix += '\n'
        }

        if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
          line = prefix + line
          ret = cli.processLine(line, true)
          prefix = ""
          currentPrompt = promptWithCurrentDB
        } else {
          prefix = prefix + line
          currentPrompt = continuedPromptWithDBSpaces
        }
      }
      line = reader.readLine(currentPrompt + "> ")
    }

    sessionState.close()

    System.exit(ret)
  }


  def isRemoteMode(state: CliSessionState): Boolean = {
    //    sessionState.isRemoteMode
    state.isHiveServerQuery
  }

}

private[hive] class SparkSQLCLIDriver extends CliDriver with Logging {
  private val sessionState = SessionState.get().asInstanceOf[CliSessionState]

  private val LOG = LogFactory.getLog("CliDriver")

  private val console = new SessionState.LogHelper(LOG)

  if (sessionState.getIsSilent) {
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  private val isRemoteMode = {
    SparkSQLCLIDriver.isRemoteMode(sessionState)
  }

  private val conf: Configuration =
    if (sessionState != null) sessionState.getConf else new Configuration()

  // Force initializing SparkSQLEnv. This is put here but not object SparkSQLCliDriver
  // because the Hive unit tests do not go through the main() code path.
  if (!isRemoteMode) {
    SparkSQLEnv.init()
  } else {
    // Hive 1.2 + not supported in CLI
    throw new RuntimeException("Remote operations not supported")
  }

  override def setHiveVariables(hiveVariables: java.util.Map[String, String]): Unit = {
    hiveVariables.asScala.foreach(kv => SparkSQLEnv.sqlContext.conf.setConfString(kv._1, kv._2))
  }

  override def processCmd(cmd: String): Int = {
    val cmd_trimmed: String = cmd.trim()
    val cmd_lower = cmd_trimmed.toLowerCase(Locale.ROOT)
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    if (cmd_lower.equals("quit") ||
      cmd_lower.equals("exit")) {
      sessionState.close()
      System.exit(0)
    }
    if (tokens(0).toLowerCase(Locale.ROOT).equals("source") ||
      cmd_trimmed.startsWith("!") || isRemoteMode) {
      val start = System.currentTimeMillis()
      super.processCmd(cmd)
      val end = System.currentTimeMillis()
      val timeTaken: Double = (end - start) / 1000.0
      console.printInfo(s"Time taken: $timeTaken seconds")
      0
    } else {
      var ret = 0
      val hconf = conf.asInstanceOf[HiveConf]
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens, hconf)

      if (proc != null) {
        // scalastyle:off println
        if (proc.isInstanceOf[Driver] || proc.isInstanceOf[SetProcessor] ||
          proc.isInstanceOf[AddResourceProcessor] || proc.isInstanceOf[ListResourceProcessor] ||
          proc.isInstanceOf[ResetProcessor] ) {
          val driver = new SparkSQLDriver

          driver.init()
          val out = sessionState.out
          val err = sessionState.err
          val start: Long = System.currentTimeMillis()
          if (sessionState.getIsVerbose) {
            out.println(cmd)
          }
          val rc = driver.run(cmd)
          val end = System.currentTimeMillis()
          val timeTaken: Double = (end - start) / 1000.0

          ret = rc.getResponseCode
          if (ret != 0) {
            // For analysis exception, only the error is printed out to the console.
            rc.getException() match {
              case e : AnalysisException =>
                err.println(s"""Error in query: ${e.getMessage}""")
              case _ => err.println(rc.getErrorMessage())
            }
            driver.close()
            return ret
          }

          val res = new JArrayList[String]()

          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
            // Print the column names.
            Option(driver.getSchema.getFieldSchemas).foreach { fields =>
              out.println(fields.asScala.map(_.getName).mkString("\t"))
            }
          }

          var counter = 0
          try {
            while (!out.checkError() && driver.getResults(res)) {
              res.asScala.foreach { l =>
                counter += 1
                out.println(l)
              }
              res.clear()
            }
          } catch {
            case e: IOException =>
              console.printError(
                s"""Failed with exception ${e.getClass.getName}: ${e.getMessage}
                   |${org.apache.hadoop.util.StringUtils.stringifyException(e)}
                 """.stripMargin)
              ret = 1
          }

          val cret = driver.close()
          if (ret == 0) {
            ret = cret
          }

          var responseMsg = s"Time taken: $timeTaken seconds"
          if (counter != 0) {
            responseMsg += s", Fetched $counter row(s)"
          }
          console.printInfo(responseMsg, null)
          // Destroy the driver to release all the locks.
          driver.destroy()
        } else {
          if (sessionState.getIsVerbose) {
            sessionState.out.println(tokens(0) + " " + cmd_1)
          }
          ret = proc.run(cmd_1).getResponseCode
        }
        // scalastyle:on println
      }
      ret
    }
  }
}

