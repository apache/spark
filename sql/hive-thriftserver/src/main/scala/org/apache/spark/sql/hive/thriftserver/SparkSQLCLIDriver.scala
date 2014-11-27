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
import org.apache.spark.sql.hive.HiveShim
import org.apache.spark.sql.hive.thriftserver.HiveThriftServerShim

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

    if (!sessionState.isRemoteMode) {
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

    val filterCommentResult = FilterCommentResult("", false, new scala.collection.mutable.Stack[String])

    while (line != null) {
      if (prefix.nonEmpty) {
        prefix += '\n'
      }

      // filter the comments in the line
      cli.filterComment(line, filterCommentResult)

      if (filterCommentResult.isCmdEnded) {
        ret = cli.processLine(filterCommentResult.cmd, true)
        filterCommentResult.cmd = ""
        currentPrompt = promptWithCurrentDB
      } else {
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
      val proc: CommandProcessor = HiveShim.getCommandProcessor(Array(tokens(0)), hconf)

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
          val end = System.currentTimeMillis()
          val timeTaken:Double = (end - start) / 1000.0

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

          console.printInfo(s"Time taken: $timeTaken seconds", null)
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

  /**
   * return a commultive string without single line comment (begin with "#" or "--") and
   * multi line comments (quoted in "/* */") through a filter function.
   * this can be used for **comment support** in command input.
   * ######three comment styles supported:
   * 1. From a '#' character to the end of the line.
   * 2. From a '--' sequence to the end of the line
   * 3. From a /* sequence to the following */ sequence, as in the C programming language.
   * This syntax allows a comment to extend over multiple lines because the beginning
   * and closing sequences need not be on the same line.
   * @param line line string inputed from console
   * @param filterCommentResult a case class that encapsulates the result of filterComment
   * for making code more concise
   * @return a case class representing result after filter comments, including (cmd, isCmdEnded, commentStack)
   */
  def filterComment(line: String,
                    filterCommentResult: FilterCommentResult
                    = FilterCommentResult("", false, new scala.collection.mutable.Stack[String])
                     ): FilterCommentResult = {
    filterCommentResult.isCmdEnded = false

    if (line == null || line.trim.isEmpty) return filterCommentResult

    // define some regexes for match
    val SINGLE_LINE_COMMENT_DASH_REGEX = """(.*?)--(.*)""".r
    val SINGLE_LINE_COMMENT_POUND_REGEX = """(.*?)#(.*)""".r
    val MULTI_LINE_COMMENT_START_REGEX = """(.*?)/\*(.*)""".r
    val MULTI_LINE_COMMENT_END_REGEX = """(.*?)\*/(.*)""".r
    val SINGLE_QUOTATION_MARK_REGEX = """(.*?)'(.*)""".r
    val DOUBLE_QUOTATION_MARK_REGEX = """(.*?)"(.*)""".r
    val CMD_END_REGEX = """(.*?);(.*)""".r

    val markerMap = scala.collection.immutable.HashMap(
      "SINGLE_LINE_COMMENT_DASH" ->(SINGLE_LINE_COMMENT_DASH_REGEX, " -- "),
      "SINGLE_LINE_COMMENT_POUND" ->(SINGLE_LINE_COMMENT_POUND_REGEX, " # "),
      "MULTI_LINE_COMMENT_START" ->(MULTI_LINE_COMMENT_START_REGEX, " /* "),
      "MULTI_LINE_COMMENT_END" ->(MULTI_LINE_COMMENT_END_REGEX, " */ "),
      "SINGLE_QUOTATION_MARK" ->(SINGLE_QUOTATION_MARK_REGEX, "'"),
      "DOUBLE_QUOTATION_MARK" ->(DOUBLE_QUOTATION_MARK_REGEX, "\""),
      "CMD_END" ->(CMD_END_REGEX, ";")
    )

    // to get the first match in the line
    var filterMatchRegex: scala.util.matching.Regex = null
    var filterMatchIdx = -1
    var filterMatchType = ""

    markerMap.foreach(x => {
      val findFirstMatch = x._2._1.findFirstMatchIn(line)
      if (findFirstMatch != None) {
        val matchPositonIdx = findFirstMatch.get.group(1).length
        if (matchPositonIdx < filterMatchIdx || filterMatchIdx == -1) {
          filterMatchType = x._1
          filterMatchRegex = x._2._1
          filterMatchIdx = matchPositonIdx
        }
      }
    })

    // process the comments one by one based on head eliment in commentStack and first match regex.
    val head = filterCommentResult.commentStack.headOption.getOrElse(null)

    if (filterMatchRegex != null) {
      val filterMatch = filterMatchRegex.findFirstMatchIn(line)
      val Array(part1, part2) = filterMatch.get.subgroups.toArray

      if (head == null) {
        filterMatchType match {
          case "CMD_END" =>
            filterCommentResult.cmd += part1 + ";"
            filterCommentResult.isCmdEnded = true
            filterCommentResult.cmd = filterCommentResult.cmd.replaceAll( """\n+\s*\n*""", "\n").trim
          case "SINGLE_QUOTATION_MARK" | "DOUBLE_QUOTATION_MARK" =>
            filterCommentResult.commentStack.push(filterMatchType)
            filterCommentResult.cmd += part1 + markerMap(filterMatchType)._2
            filterComment(part2, filterCommentResult)
          case "SINGLE_LINE_COMMENT_DASH" | "SINGLE_LINE_COMMENT_POUND" =>
            filterCommentResult.cmd += part1
          case "MULTI_LINE_COMMENT_START" =>
            filterCommentResult.commentStack.push(filterMatchType)
            filterCommentResult.cmd += part1
            filterComment(part2, filterCommentResult)
          case "MULTI_LINE_COMMENT_END" =>
            throw new Exception("found no matched multi line comment start marker for */.")
          case _ =>
        }

      } else {
        head match {
          case "SINGLE_QUOTATION_MARK" => {
            if (filterMatchType == "SINGLE_QUOTATION_MARK") {
              filterCommentResult.cmd += part1 + "'"
              filterCommentResult.commentStack.pop()
            } else if (filterMatchType == "DOUBLE_QUOTATION_MARK") {
              filterCommentResult.cmd += part1 + "\""
              filterCommentResult.commentStack.push(filterMatchType)
            } else {
              filterCommentResult.cmd += part1 + markerMap(filterMatchType)._2
            }
            filterComment(part2, filterCommentResult)
          }
          case "DOUBLE_QUOTATION_MARK" => {
            if (filterMatchType == "SINGLE_QUOTATION_MARK") {
              filterCommentResult.cmd += part1 + "'"
              filterCommentResult.commentStack.push(filterMatchType)
            } else if (filterMatchType == "DOUBLE_QUOTATION_MARK") {
              filterCommentResult.cmd += part1 + "\""
              filterCommentResult.commentStack.pop()
            } else {
              filterCommentResult.cmd += part1 + markerMap(filterMatchType)._2
            }
            filterComment(part2, filterCommentResult)
          }
          case "MULTI_LINE_COMMENT_START" => {
            if (filterMatchType == "MULTI_LINE_COMMENT_END") filterCommentResult.commentStack.pop()
            filterComment(part2, filterCommentResult)
          }
        }
      }
    } else {
      // filterMatchRegex == null
      if (head == null) {
        filterCommentResult.cmd += line
      } else {
        if (head == "SINGLE_QUOTATION_MARK" || head == "DOUBLE_QUOTATION_MARK") {
          filterCommentResult.cmd += line
        }
      }
    }
    filterCommentResult
  }
}

/**
 * using for encapsulating the result of filterComment.
 * @param cmd a commulative string for generating a cmd from multi inputs
 * @param isCmdEnded  whether the cmd is ended.
 * @param commentStack  a stack that acts as a context, stores the current elements that should be considered
 *                      while filtering comments, like ', ", or the begin flag of multi line comments
 */
case class FilterCommentResult(var cmd: String,
                               var isCmdEnded: Boolean,
                               var commentStack: scala.collection.mutable.Stack[String])