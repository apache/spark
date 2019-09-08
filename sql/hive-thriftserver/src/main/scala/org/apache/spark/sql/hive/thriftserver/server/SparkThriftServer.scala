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

package org.apache.spark.sql.hive.thriftserver.server

import java.util

import scala.collection.JavaConverters._

import org.apache.commons.cli._
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.common.util.{HiveStringUtils, HiveVersionInfo}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.thriftserver.{CompositeService, SparkSQLEnv}
import org.apache.spark.sql.hive.thriftserver.cli.CLIService
import org.apache.spark.sql.hive.thriftserver.cli.thrift.{ThriftBinaryCLIService, ThriftCLIService,
  ThriftHttpCLIService}
import org.apache.spark.util.ShutdownHookManager

class SparkThriftServer(sqlContext: SQLContext)
  extends CompositeService(classOf[SparkThriftServer].getSimpleName)
    with Logging {

  private var cliService: CLIService = null
  private var thriftCLIService: ThriftCLIService = null

  try {
    HiveConf.setLoadHiveServer2Config(true)
  } catch {
    case e: Throwable => e.printStackTrace()
  }

  override def init(hiveConf: HiveConf): Unit = {
    cliService = new CLIService(this, sqlContext)
    addService(cliService)
    if (SparkThriftServer.isHTTPTransportMode(hiveConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService)
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService)
    }
    addService(thriftCLIService)
    super.init(hiveConf)
    // Add a shutdown hook for catching SIGTERM & SIGINT
    // this must be higher than the Hadoop Filesystem priority of 10,
    // which the default priority is.
    // The signature of the callback must match that of a scala () -> Unit
    // function
    ShutdownHookManager.addShutdownHook(() => {
      try {
        logInfo("Hive Server Shutdown hook invoked")
        stop()
      } catch {
        case e: Throwable =>
          logWarning("Ignoring Exception while stopping Hive Server from shutdown hook", e)
      }
    })
  }


  override def start(): Unit = {
    super.start()
  }

  override def stop(): Unit = {
    logInfo("Shutting down HiveServer2")
    super.stop()
  }
}

object SparkThriftServer extends Logging {
  @throws[Throwable]
  private def startHiveServer2(): Unit = {
    var attempts = 0
    var maxAttempts: Long = 1
    while (true) {
      logInfo("Starting HiveServer2")
      val hiveConf = new HiveConf
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS)
      var server: SparkThriftServer = null
      try {
        server = new SparkThriftServer(SparkSQLEnv.sqlContext)
        server.init(hiveConf)
        server.start()

        // ToDo add a JVMPauseMonitor for spark

      } catch {
        case throwable: Throwable =>
          if (server != null) {
            try
              server.stop()
            catch {
              case t: Throwable =>
                logInfo("Exception caught when calling stop of HiveServer2 " +
                  "before retrying start", t)
            } finally server = null
          }
          if ( {
            attempts += 1;
            attempts
          } >= maxAttempts) {
            throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable)
          } else {
            logWarning("Error starting HiveServer2 on attempt " +
              attempts + ", will retry in 60 seconds", throwable)
            try
              Thread.sleep(60L * 1000L)
            catch {
              case e: InterruptedException =>
                Thread.currentThread.interrupt()
            }
          }
      }
    }
  }

  def isHTTPTransportMode(hiveConf: HiveConf): Boolean = {
    var transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE")
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    }
    if (transportMode != null && transportMode.equalsIgnoreCase("http")) {
      return true
    }
    false
  }

  def main(args: Array[String]): Unit = {
    HiveConf.setLoadHiveServer2Config(true)
    try {
      val oproc = new ServerOptionsProcessor("hiveserver2")
      val oprocResponse = oproc.parse(args)
      // NOTE: It is critical to do this here so that log4j is reinitialized
      // before any of the other core hive classes are loaded
      val initLog4jMessage = LogUtils.initHiveLog4j
      val classname = classOf[SparkThriftServer].getSimpleName
      logDebug(initLog4jMessage)
      logInfo(toStartupShutdownString("STARTUP_MSG: ",
        Array[String]("Starting " + classname,
          "  host = " + HiveStringUtils.getHostname,
          "  args = " + util.Arrays.asList(args),
          "  version = " + HiveVersionInfo.getVersion,
          "  classpath = " + System.getProperty("java.class.path"),
          "  build = " + HiveVersionInfo.getUrl +
            " -r " + HiveVersionInfo.getRevision +
            "; compiled by '" + HiveVersionInfo.getUser +
            "' on " + HiveVersionInfo.getDate)))
      ShutdownHookManager.addShutdownHook(0)(() => {
        logInfo(toStartupShutdownString(
          "SHUTDOWN_MSG: ",
          Array[String]("Shutting down " + classname +
            " at " + HiveStringUtils.getHostname)))
      })
      // Log debug message from "oproc" after log4j initialize properly
      logDebug(oproc.getDebugMessage.toString)
      // Call the executor which will execute the appropriate command based on the parsed options
      oprocResponse.getServerOptionsExecutor.execute()
    } catch {
      case e: LogUtils.LogInitializationException =>
        logError("Error initializing log: " + e.getMessage, e)
        System.exit(-1)
    }
  }


  private def toStartupShutdownString(prefix: String, msg: Array[String]) = {
    val b = new StringBuilder(prefix)
    b.append("\n/************************************************************")
    val arr = msg
    val len = msg.length
    var i = 0
    while (i < len) {
      val s = arr(i)
      b.append("\n" + prefix + s)
      i += 1
    }
    b.append("\n************************************************************/")
    b.toString
  }

  /**
   * ServerOptionsProcessor.
   * Process arguments given to HiveServer2 (-hiveconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  @SuppressWarnings(Array("static-access"))
  class ServerOptionsProcessor(val serverName: String) {
    // -hiveconf x=y
    final private val options = new Options
    private var commandLine: CommandLine = null
    final private val debugMessage = new StringBuilder
    OptionBuilder.withValueSeparator
    OptionBuilder.hasArgs(2)
    OptionBuilder.withArgName("property=value")
    OptionBuilder.withLongOpt("hiveconf")
    OptionBuilder.withDescription("Use value for given property")
    options.addOption(OptionBuilder.create)
    options.addOption(
      new Option("H",
        "help",
        false,
        "Print help information"))


    def parse(argv: Array[String]): ServerOptionsProcessorResponse = {
      try {
        commandLine = new GnuParser().parse(options, argv)
        // Process --hiveconf
        // Get hiveconf param values and set the System property values
        val confProps = commandLine.getOptionProperties("hiveconf")
        for (propKey <- confProps.stringPropertyNames.asScala) {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n")
          System.setProperty(propKey, confProps.getProperty(propKey))
        }
        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options))
        }
      } catch {
        case e: ParseException =>
          // Error out & exit - we were not able to parse the args successfully
          logError("Error starting HiveServer2 with given arguments: ")
          logError(e.getMessage)
          System.exit(-1)
      }
      // Default executor, when no option is specified
      new ServerOptionsProcessorResponse(new StartOptionExecutor)
    }

    private[server] def getDebugMessage = debugMessage
  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  private[server] class ServerOptionsProcessorResponse(
                   val serverOptionsExecutor: ServerOptionsExecutor) {
    private[server] def getServerOptionsExecutor = serverOptionsExecutor
  }

  /**
   * The executor interface for running the appropriate HiveServer2 command based on parsed options
   */
  private[server] trait ServerOptionsExecutor {
    def execute(): Unit
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  private[server] class HelpOptionExecutor(val serverName: String,
                                           val options: Options)
    extends ServerOptionsExecutor {
    override def execute(): Unit = {
      new HelpFormatter().printHelp(serverName, options)
      System.exit(0)
    }
  }

  /**
   * StartOptionExecutor: starts HiveServer2.
   * This is the default executor, when no option is specified.
   */
  private[server] class StartOptionExecutor extends ServerOptionsExecutor {
    override def execute(): Unit = {
      try
        startHiveServer2()
      catch {
        case t: Throwable =>
          logError("Error starting HiveServer2", t)
          System.exit(-1)
      }
    }
  }

}
