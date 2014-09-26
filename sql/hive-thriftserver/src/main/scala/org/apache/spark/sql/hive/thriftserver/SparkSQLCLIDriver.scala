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

import scala.collection.JavaConversions._

import java.io._
import java.util.{ArrayList => JArrayList}

import jline.{ConsoleReader, History}
import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.common.{HiveInterruptCallback, HiveInterruptUtils, LogUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.processors.{SetProcessor, CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.thrift.transport.TSocket

import org.apache.spark.Logging

private[hive] object SparkSQLCLIDriver {
  private var prompt = "spark-sql"
  private var continuedPrompt = "".padTo(prompt.length, ' ')
  private var transport:TSocket = _

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

    val sessionState = new CliSessionState(new HiveConf(classOf[SessionState]))

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
    sessionState.cmdProperties.entrySet().foreach { item: java.util.Map.Entry[Object, Object] =>
      conf.set(item.getKey.asInstanceOf[String], item.getValue.asInstanceOf[String])
      sessionState.getOverriddenConfigurations.put(
        item.getKey.asInstanceOf[String], item.getValue.asInstanceOf[String])
    }

    SessionState.start(sessionState)

    // Clean up after we exit
    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          SparkSQLEnv.stop()
        }
      }
    )

    // "-h" option has been passed, so connect to Hive thrift server.
    if (sessionState.getHost != null) {
      sessionState.connect()
      if (sessionState.isRemoteMode) {
        prompt = s"[${sessionState.getHost}:${sessionState.getPort}]" + prompt
        continuedPrompt = "".padTo(prompt.length, ' ')
      }
    }

    if (!sessionState.isRemoteMode && !ShimLoader.getHadoopShims.usesJobShell()) {
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

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(sessionState)

    if (sessionState.execString != null) {
      System.exit(cli.processLine(sessionState.execString))
    }

    try {
      if (sessionState.fileName != null) {
        System.exit(cli.processFile(sessionState.fileName))
      }
    } catch {
      case e: FileNotFoundException =>
        System.err.println(s"Could not open input file for reading. (${e.getMessage})")
        System.exit(3)
    }

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    CliDriver.getCommandCompletor.foreach((e) => reader.addCompletor(e))

    val historyDirectory = System.getProperty("user.home")

    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + ".hivehistory"
        reader.setHistory(new History(new File(historyFile)))
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                           "history file.  History will not be available during this session.")
        System.err.println(e.getMessage)
    }

    val clientTransportTSocketField = classOf[CliSessionState].getDeclaredField("transport")
    clientTransportTSocketField.setAccessible(true)

    transport = clientTransportTSocketField.get(sessionState).asInstanceOf[TSocket]

    var ret = 0
    var prefix = ""
    val currentDB = ReflectionUtils.invokeStatic(classOf[CliDriver], "getFormattedDb",
      classOf[HiveConf] -> conf, classOf[CliSessionState] -> sessionState)

    def promptWithCurrentDB = s"$prompt$currentDB"
    def continuedPromptWithDBSpaces = continuedPrompt + ReflectionUtils.invokeStatic(
      classOf[CliDriver], "spacesForString", classOf[String] -> currentDB)

    var currentPrompt = promptWithCurrentDB
    var line = reader.readLine(currentPrompt + "> ")

    while (line != null) {
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

      line = reader.readLine(currentPrompt + "> ")
    }

    sessionState.close()

    System.exit(ret)
  }
}

private[hive] class SparkSQLCLIDriver extends CliDriver with Logging {
  private val sessionState = SessionState.get().asInstanceOf[CliSessionState]

  private val LOG = LogFactory.getLog("CliDriver")

  private val console = new SessionState.LogHelper(LOG)

  private val conf: Configuration =
    if (sessionState != null) sessionState.getConf else new Configuration()

  // Force initializing SparkSQLEnv. This is put here but not object SparkSQLCliDriver
  // because the Hive unit tests do not go through the main() code path.
  if (!sessionState.isRemoteMode) {
    SparkSQLEnv.init()
  }

  override def processCmd(cmd: String): Int = {
    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    if (cmd_trimmed.toLowerCase.equals("quit") ||
      cmd_trimmed.toLowerCase.equals("exit") ||
      tokens(0).equalsIgnoreCase("source") ||
      cmd_trimmed.startsWith("!") ||
      tokens(0).toLowerCase.equals("list") ||
      sessionState.isRemoteMode) {
      val start = System.currentTimeMillis()
      super.processCmd(cmd)
      val end = System.currentTimeMillis()
      val timeTaken: Double = (end - start) / 1000.0
      console.printInfo(s"Time taken: $timeTaken seconds")
      0
    } else {
      var ret = 0
      val hconf = conf.asInstanceOf[HiveConf]
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hconf)

      if (proc != null) {
        if (proc.isInstanceOf[Driver] || proc.isInstanceOf[SetProcessor]) {
          val driver = new SparkSQLDriver

          driver.init()
          val out = sessionState.out
          val start:Long = System.currentTimeMillis()
          if (sessionState.getIsVerbose) {
            out.println(cmd)
          }

          val rc = driver.run(cmd)
          ret = rc.getResponseCode
          if (ret != 0) {
            console.printError(rc.getErrorMessage())
            driver.close()
            return ret
          }

          val res = new JArrayList[String]()

          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
            // Print the column names.
            Option(driver.getSchema.getFieldSchemas).map { fields =>
              out.println(fields.map(_.getName).mkString("\t"))
            }
          }

          try {
            while (!out.checkError() && driver.getResults(res)) {
              res.foreach(out.println)
              res.clear()
            }
          } catch {
            case e:IOException =>
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

          val end = System.currentTimeMillis()
          if (end > start) {
            val timeTaken:Double = (end - start) / 1000.0
            console.printInfo(s"Time taken: $timeTaken seconds", null)
          }

          // Destroy the driver to release all the locks.
          driver.destroy()
        } else {
          if (sessionState.getIsVerbose) {
            sessionState.out.println(tokens(0) + " " + cmd_1)
          }
          ret = proc.run(cmd_1).getResponseCode
        }
      }
      ret
    }
  }
}

