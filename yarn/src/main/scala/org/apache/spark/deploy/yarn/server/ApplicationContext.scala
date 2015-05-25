package org.apache.spark.deploy.yarn.server

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.SparkListener

object ApplicationContext {

  val SPARK_APP_NAME = "spark.app.name"
  val SPARK_NUMBER_EXECUTORS = "spark.executor.instances"
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  val SPARK_EXECUTOR_EXTRA_JAVA_OPTS = "spark.executor.extraJavaOpts"

  val YARN_REPORT_INTERVAL = "yarn.report.interval"
  val SPARK_YARN_REPORT_INTERVAL = s"spark.$YARN_REPORT_INTERVAL"

  val YARN_APP_HANDSHAKE_TIMEOUT = "app.yarn.handshake.timeout"
  val SPARK_YARN_APP_HANDSHAKE_TIMEOUT = s"spark.$YARN_APP_HANDSHAKE_TIMEOUT"
  val SPARK_APP_CHANNEL_PROTOCOL = "spark.yarn.app.channel.protocol"

  val SPARK_EVENT_LOG_ENABLED = "spark.eventLog.enabled"
  val SPARK_DEPLOY_MODE = "spark.deployMode"

  val APP_JAR = "app.jar"
  val SPARK_ADD_JARS = "spark.addJars"
  val SPARK_MAIN_CLASS = "spark.mainClass"
  val CONF_SPARK_JAR = "spark.yarn.jar"

  val SPARK_JAR = "SPARK_JAR"
  val SPARK_YARN_MODE = "SPARK_YARN_MODE"
  val SPARK_YARN_APP_ID = "spark.yarn.app.id"
  val SPARK_APP_INFO = "spark.app.info"
  val SPARK_APP_COMPLETED = "spark.app.completed"
  val SPARK_SHOW_CONF = "spark.show.conf"
  val SPARK_YARN_APP_DEBUG_ENABLED = "spark.app.yarn.debug.enabled"
  val SPARK_YARN_APP_SLEEP_TIME_BEFORE_COMPLETE= "spark.app.yarn.sleep.time.before.job.completed"

}

case object Ping
case object Pong



class ApplicationContext(val conf: SparkConf) {
  import ApplicationContext._

  val appName = conf.get(SPARK_APP_NAME, "Spark Application")
  val deployMode = conf.get(SPARK_DEPLOY_MODE)
  val sparkCtx: SparkContext = new SparkContext(deployMode, appName, conf)
  val messenger  = createAppChannelMessenger()
  val logger = new ChannelMessageLogger(appName, Some(this))
  val stdOut = Console.out
  val stdErr = Console.err

  //add spark listener
  sparkCtx.addSparkListener(new JobProgressRelayListener(this))
  Console.setOut(new RedirectPrintStream(logger, stdOut))
  Console.setErr(new RedirectPrintStream(logger, stdErr))

  showConf()

  def showConf(): Unit = {
    if (conf.get(SPARK_SHOW_CONF, "true").toBoolean) {
      println(conf.toDebugString)
    }
  }

  def restoreConsoleOut() = {
    Console.setOut(stdOut)
    Console.setOut(stdErr)
  }

  def addSparkListener(listener: SparkListener) {
    sparkCtx.addSparkListener(listener)
  }

  def stop() {
    println("[ApplicationContext] stopping ")
    CommunicationHelper.stopRelayMessenger(Some(sparkCtx), messenger)
    sparkCtx.stop()
  }

  private def createAppChannelMessenger(): ChannelMessenger  =
    CommunicationHelper.createRelayMessenger(this)

}