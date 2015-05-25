package org.apache.spark.deploy.yarn.server

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn._
import org.apache.spark.deploy.yarn.server.ApplicationContext._



trait YarnSparkApp  {


  def sparkMain(appCtx: ApplicationContext)

  def run(conf: SparkConf) {

    var logger = new ChannelMessageLogger("spark app", None)

    logTime { // log time
      var failed = false
      var appContext: Option[ApplicationContext] = None

      try {
        appContext = Some(new ApplicationContext(conf))
        //update logger update
        logger = appContext.get.logger
        logger.logInfo(s"starting ${appContext.get.appName}")
        sparkMain(appContext.get)

      } catch {
        case e: Throwable =>
          failed = true
          val t = wrapThrowable(appContext, e)
          printStackTrace(t)
          throw t
      } finally {
        if (!failed) {
          logger.sendUpdateMessage(SPARK_APP_COMPLETED, true)
          logger.logInfo(s"spark app finished.")
        }
        waitToFinish(failed)
        appContext.foreach(_.stop())
        appContext.foreach(_.restoreConsoleOut())
      }

      def waitToFinish(failed: Boolean) {
        val debugEnabled = conf.get(SPARK_YARN_APP_DEBUG_ENABLED, "false").toBoolean
        if (debugEnabled || failed) {
          val sleepTime = conf.get(SPARK_YARN_APP_SLEEP_TIME_BEFORE_COMPLETE, "0").toInt
          val message: String = s" sleeping for $sleepTime sec before stop"
          logger.logInfo(message)
          appContext.foreach { aCtx =>
            logger.sendVisualMessage(DisplayMessage(s" Wait -- ", message))
          }
          TimeUnit.SECONDS.sleep(sleepTime)
        }
      }
    }


    def logTime[T] ( f :  => T) : T = {
      val start = System.currentTimeMillis()
      val t = f
      val end = System.currentTimeMillis()
      val msg = "total running time: " + (end - start) / 1000 + " sec"
      logger.logInfo(msg)
      t
    }
  }


  def wrapThrowable(appContext: Option[ApplicationContext], e: Throwable): RuntimeException = {
    val appName = appContext.map(ac => ac.appName).getOrElse("")
    val failedMsg = s"Application $appName failed due to " +
      s"${if (e.getMessage == null && e.getCause != null) e.getCause.getMessage else e.getMessage}"
    new RuntimeException(failedMsg, e)
  }

  def printStackTrace(t: Throwable) {
    ResourceUtil.withResource(new StringWriter()) { s =>
      ResourceUtil.withResource(new PrintWriter(s)) { p =>
        t.printStackTrace(p)
        Console.err.println(s.getBuffer.toString)
      }
    }
  }

}
