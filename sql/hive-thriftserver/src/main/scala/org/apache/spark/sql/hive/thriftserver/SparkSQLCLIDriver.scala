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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{ArrayList => JArrayList, List => JList, Locale}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import jline.console.ConsoleReader
import jline.console.history.FileHistory
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.HiveInterruptUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.thrift.transport.TSocket
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.security.HiveDelegationTokenProvider
import org.apache.spark.sql.internal.SharedState
import org.apache.spark.util.ShutdownHookManager

/**
 * This code doesn't support remote connections in Hive 1.2+, as the underlying CliDriver
 * has dropped its support.
 */
private[hive] object SparkSQLCLIDriver extends Logging {
  private val prompt = "spark-sql"
  private val continuedPrompt = "".padTo(prompt.length, ' ')
  private var transport: TSocket = _
  private final val SPARK_HADOOP_PROP_PREFIX = "spark.hadoop."

  initializeLogIfNecessary(true)
  installSignalHandler()

  /**
   * Install an interrupt callback to cancel all Spark jobs. In Hive's CliDriver#processLine(),
   * a signal handler will invoke this registered callback if a Ctrl+C signal is detected while
   * a command is being processed by the current thread.
   */
  def installSignalHandler(): Unit = {
    HiveInterruptUtils.add(() => {
      // Handle remote execution mode
      if (SparkSQLEnv.sparkContext != null) {
        SparkSQLEnv.sparkContext.cancelAllJobs()
      } else {
        if (transport != null) {
          // Force closing of TCP connection upon session termination
          transport.getSocket.close()
        }
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val oproc = new OptionsProcessor()
    if (!oproc.process_stage1(args)) {
      System.exit(1)
    }

    val sparkConf = new SparkConf(loadDefaults = true)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val extraConfigs = HiveUtils.formatTimeVarsForHiveClient(hadoopConf)

    val cliConf = HiveClientImpl.newHiveConf(sparkConf, hadoopConf, extraConfigs)

    val sessionState = new CliSessionState(cliConf)

    sessionState.in = System.in
    try {
      sessionState.out = new PrintStream(System.out, true, UTF_8.name())
      sessionState.info = new PrintStream(System.err, true, UTF_8.name())
      sessionState.err = new PrintStream(System.err, true, UTF_8.name())
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(sessionState)) {
      System.exit(2)
    }

    // Set all properties specified via command line.
    val conf: HiveConf = sessionState.getConf
    // Hive 2.0.0 onwards HiveConf.getClassLoader returns the UDFClassLoader (created by Hive).
    // Because of this spark cannot find the jars as class loader got changed
    // Hive changed the class loader because of HIVE-11878, so it is required to use old
    // classLoader as sparks loaded all the jars in this classLoader
    conf.setClassLoader(Thread.currentThread().getContextClassLoader)
    sessionState.cmdProperties.entrySet().asScala.foreach { item =>
      val key = item.getKey.toString
      val value = item.getValue.toString
      // We do not propagate metastore options to the execution copy of hive.
      if (key != "javax.jdo.option.ConnectionURL") {
        conf.set(key, value)
        sessionState.getOverriddenConfigurations.put(key, value)
      }
    }

    val tokenProvider = new HiveDelegationTokenProvider()
    if (tokenProvider.delegationTokensRequired(sparkConf, hadoopConf)) {
      val credentials = new Credentials()
      tokenProvider.obtainDelegationTokens(hadoopConf, sparkConf, credentials)
      UserGroupInformation.getCurrentUser.addCredentials(credentials)
    }

    val warehousePath = SharedState.resolveWarehousePath(sparkConf, conf)
    val qualified = SharedState.qualifyWarehousePath(conf, warehousePath)
    SharedState.setWarehousePathConf(sparkConf, conf, qualified)
    SessionState.setCurrentSessionState(sessionState)

    // Clean up after we exit
    ShutdownHookManager.addShutdownHook { () => SparkSQLEnv.stop() }

    if (isRemoteMode(sessionState)) {
      // Hive 1.2 + not supported in CLI
      throw QueryExecutionErrors.remoteOperationsUnsupportedError()
    }
    // Respect the configurations set by --hiveconf from the command line
    // (based on Hive's CliDriver).
    val hiveConfFromCmd = sessionState.getOverriddenConfigurations.entrySet().asScala
    val newHiveConf = hiveConfFromCmd.map { kv =>
      // If the same property is configured by spark.hadoop.xxx, we ignore it and
      // obey settings from spark properties
      val k = kv.getKey
      val v = sys.props.getOrElseUpdate(SPARK_HADOOP_PROP_PREFIX + k, kv.getValue)
      (k, v)
    }

    val cli = new SparkSQLCLIDriver
    cli.setHiveVariables(oproc.getHiveVariables)

    // In SparkSQL CLI, we may want to use jars augmented by hiveconf
    // hive.aux.jars.path, here we add jars augmented by hiveconf to
    // Spark's SessionResourceLoader to obtain these jars.
    val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
    if (StringUtils.isNotBlank(auxJars)) {
      val resourceLoader = SparkSQLEnv.sqlContext.sessionState.resourceLoader
      StringUtils.split(auxJars, ",").foreach(resourceLoader.addJar(_))
    }

    // The class loader of CliSessionState's conf is current main thread's class loader
    // used to load jars passed by --jars. One class loader used by AddJarsCommand is
    // sharedState.jarClassLoader which contain jar path passed by --jars in main thread.
    // We set CliSessionState's conf class loader to sharedState.jarClassLoader.
    // Thus we can load all jars passed by --jars and AddJarsCommand.
    sessionState.getConf.setClassLoader(SparkSQLEnv.sqlContext.sharedState.jarClassLoader)

    // TODO work around for set the log output to console, because the HiveContext
    // will set the output into an invalid buffer.
    sessionState.in = System.in
    try {
      sessionState.out = new PrintStream(System.out, true, UTF_8.name())
      sessionState.info = new PrintStream(System.err, true, UTF_8.name())
      sessionState.err = new PrintStream(System.err, true, UTF_8.name())
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (sessionState.database != null) {
      SparkSQLEnv.sqlContext.sessionState.catalog.setCurrentDatabase(
        s"${sessionState.database}")
    }

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(sessionState)

    // We don't propagate hive.metastore.warehouse.dir, because it might has been adjusted in
    // [[SharedState.loadHiveConfFile]] based on the user specified or default values of
    // spark.sql.warehouse.dir and hive.metastore.warehouse.dir.
    for ((k, v) <- newHiveConf if k != "hive.metastore.warehouse.dir") {
      SparkSQLEnv.sqlContext.setConf(k, v)
    }

    cli.printMasterAndAppId

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
    CliDriver.getCommandCompleter.foreach(reader.addCompleter)

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

  private val LOG = LoggerFactory.getLogger(classOf[SparkSQLCLIDriver])

  private val console = new SessionState.LogHelper(LOG)

  private val isRemoteMode = {
    SparkSQLCLIDriver.isRemoteMode(sessionState)
  }

  private val conf: Configuration =
    if (sessionState != null) sessionState.getConf else new Configuration()

  // Force initializing SparkSQLEnv. This is put here but not object SparkSQLCliDriver
  // because the Hive unit tests do not go through the main() code path.
  if (!isRemoteMode) {
    SparkSQLEnv.init()
    if (sessionState.getIsSilent) {
      SparkSQLEnv.sparkContext.setLogLevel("warn")
    }
  } else {
    // Hive 1.2 + not supported in CLI
    throw QueryExecutionErrors.remoteOperationsUnsupportedError()
  }

  override def setHiveVariables(hiveVariables: java.util.Map[String, String]): Unit = {
    hiveVariables.asScala.foreach(kv => SparkSQLEnv.sqlContext.conf.setConfString(kv._1, kv._2))
  }

  def printMasterAndAppId(): Unit = {
    val master = SparkSQLEnv.sparkContext.master
    val appId = SparkSQLEnv.sparkContext.applicationId
    console.printInfo(s"Spark master: $master, Application Id: $appId")
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
      val startTimeNs = System.nanoTime()
      super.processCmd(cmd)
      val endTimeNs = System.nanoTime()
      val timeTaken: Double = TimeUnit.NANOSECONDS.toMillis(endTimeNs - startTimeNs) / 1000.0
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
          proc.isInstanceOf[DeleteResourceProcessor] ||
          proc.isInstanceOf[ResetProcessor] ) {
          val driver = new SparkSQLDriver

          driver.init()
          val out = sessionState.out
          val err = sessionState.err
          val startTimeNs: Long = System.nanoTime()
          if (sessionState.getIsVerbose) {
            out.println(cmd)
          }
          val rc = driver.run(cmd)
          val endTimeNs = System.nanoTime()
          val timeTaken: Double = TimeUnit.NANOSECONDS.toMillis(endTimeNs - startTimeNs) / 1000.0

          ret = rc.getResponseCode
          if (ret != 0) {
            rc.getException match {
              case e: AnalysisException => e.cause match {
                case Some(_) if !sessionState.getIsSilent =>
                  err.println(
                    s"""Error in query: ${e.getMessage}
                       |${org.apache.hadoop.util.StringUtils.stringifyException(e)}
                     """.stripMargin)
                // For analysis exceptions in silent mode or simple ones that only related to the
                // query itself, such as `NoSuchDatabaseException`, only the error is printed out
                // to the console.
                case _ => err.println(s"""Error in query: ${e.getMessage}""")
              }
              case _ => err.println(rc.getErrorMessage())
            }
            driver.close()
            return ret
          }

          val res = new JArrayList[String]()

          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER) ||
              SparkSQLEnv.sqlContext.conf.cliPrintHeader) {
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

  // Adapted processLine from Hive 2.3's CliDriver.processLine.
  override def processLine(line: String, allowInterrupting: Boolean): Int = {
    var oldSignal: SignalHandler = null
    var interruptSignal: Signal = null

    if (allowInterrupting) {
      // Remember all threads that were running at the time we started line processing.
      // Hook up the custom Ctrl+C handler while processing this line
      interruptSignal = new Signal("INT")
      oldSignal = Signal.handle(interruptSignal, new SignalHandler() {
        private var interruptRequested: Boolean = false

        override def handle(signal: Signal): Unit = {
          val initialRequest = !interruptRequested
          interruptRequested = true

          // Kill the VM on second ctrl+c
          if (!initialRequest) {
            console.printInfo("Exiting the JVM")
            System.exit(127)
          }

          // Interrupt the CLI thread to stop the current statement and return
          // to prompt
          console.printInfo("Interrupting... Be patient, this might take some time.")
          console.printInfo("Press Ctrl+C again to kill JVM")

          HiveInterruptUtils.interrupt()
        }
      })
    }

    try {
      var lastRet: Int = 0

      // we can not use "split" function directly as ";" may be quoted
      val commands = splitSemiColon(line).asScala
      var command: String = ""
      for (oneCmd <- commands) {
        if (StringUtils.endsWith(oneCmd, "\\")) {
          command += StringUtils.chop(oneCmd) + ";"
        } else {
          command += oneCmd
          if (!StringUtils.isBlank(command)) {
            val ret = processCmd(command)
            command = ""
            lastRet = ret
            val ignoreErrors = HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIIGNOREERRORS)
            if (ret != 0 && !ignoreErrors) {
              CommandProcessorFactory.clean(conf.asInstanceOf[HiveConf])
              return ret
            }
          }
        }
      }
      CommandProcessorFactory.clean(conf.asInstanceOf[HiveConf])
      lastRet
    } finally {
      // Once we are done processing the line, restore the old handler
      if (oldSignal != null && interruptSignal != null) {
        Signal.handle(interruptSignal, oldSignal)
      }
    }
  }

  // Adapted splitSemiColon from Hive 2.3's CliDriver.splitSemiColon.
  // Note: [SPARK-31595] if there is a `'` in a double quoted string, or a `"` in a single quoted
  // string, the origin implementation from Hive will not drop the trailing semicolon as expected,
  // hence we refined this function a little bit.
  // Note: [SPARK-33100] Ignore a semicolon inside a bracketed comment in spark-sql.
  private[hive] def splitSemiColon(line: String): JList[String] = {
    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideSimpleComment = false
    var bracketedCommentLevel = 0
    var escape = false
    var beginIndex = 0
    var leavingBracketedComment = false
    var isStatement = false
    val ret = new JArrayList[String]

    def insideBracketedComment: Boolean = bracketedCommentLevel > 0
    def insideComment: Boolean = insideSimpleComment || insideBracketedComment
    def statementInProgress(index: Int): Boolean = isStatement || (!insideComment &&
      index > beginIndex && !s"${line.charAt(index)}".trim.isEmpty)

    for (index <- 0 until line.length) {
      // Checks if we need to decrement a bracketed comment level; the last character '/' of
      // bracketed comments is still inside the comment, so `insideBracketedComment` must keep true
      // in the previous loop and we decrement the level here if needed.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1
        leavingBracketedComment = false
      }

      if (line.charAt(index) == '\'' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote
        }
      } else if (line.charAt(index) == '\"' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote
        }
      } else if (line.charAt(index) == '-') {
        val hasNext = index + 1 < line.length
        if (insideDoubleQuote || insideSingleQuote || insideComment) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if it's inside quotes.
        } else if (hasNext && line.charAt(index + 1) == '-') {
          // ignore quotes and ; in simple comment
          insideSimpleComment = true
        }
      } else if (line.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideComment) {
          // do not split
        } else {
          if (isStatement) {
            // split, do not include ; itself
            ret.add(line.substring(beginIndex, index))
          }
          beginIndex = index + 1
          isStatement = false
        }
      } else if (line.charAt(index) == '\n') {
        // with a new line the inline simple comment should end.
        if (!escape) {
          insideSimpleComment = false
        }
      } else if (line.charAt(index) == '/' && !insideSimpleComment) {
        val hasNext = index + 1 < line.length
        if (insideSingleQuote || insideDoubleQuote) {
          // Ignores '/' in any case of quotes
        } else if (insideBracketedComment && line.charAt(index - 1) == '*' ) {
          // Decrements `bracketedCommentLevel` at the beginning of the next loop
          leavingBracketedComment = true
        } else if (hasNext && line.charAt(index + 1) == '*') {
          bracketedCommentLevel += 1
        }
      }
      // set the escape
      if (escape) {
        escape = false
      } else if (line.charAt(index) == '\\') {
        escape = true
      }

      isStatement = statementInProgress(index)
    }
    // Check the last char is end of nested bracketed comment.
    val endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1
    // Spark SQL support simple comment and nested bracketed comment in query body.
    // But if Spark SQL receives a comment alone, it will throw parser exception.
    // In Spark SQL CLI, if there is a completed comment in the end of whole query,
    // since Spark SQL CLL use `;` to split the query, CLI will pass the comment
    // to the backend engine and throw exception. CLI should ignore this comment,
    // If there is an uncompleted statement or an uncompleted bracketed comment in the end,
    // CLI should also pass this part to the backend engine, which may throw an exception
    // with clear error message.
    if (!endOfBracketedComment && (isStatement || insideBracketedComment)) {
      ret.add(line.substring(beginIndex))
    }
    ret
  }
}

