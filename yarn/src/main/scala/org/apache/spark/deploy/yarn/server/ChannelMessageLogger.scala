package org.apache.spark.deploy.yarn.server

import java.util.Date

/**
 * Messages are categorized as
 * LogMessage -- write to the log on the application side
 * UIMessage  -- display to the UI
 * AppMessage -- update app on the application side
 *
 * User: chester
 * Date: 7/9/14
 * Time: 7:49 AM
 */

//message are sent over the wire and logged on the agent side.
trait LogMessage {
  val time: Long
  val name: String
  val message: String
  override def toString: String = s"[$time-$name]:$message"
}

case class InfoMessage(message: String,
                       name: String,
                       time: Long = new Date().getTime) extends LogMessage
case class WarnMessage(message: String,
                       name: String,
                       time: Long = new Date().getTime) extends LogMessage
case class DebugMessage(message: String,
                        name: String,
                        time: Long = new Date().getTime) extends LogMessage
case class ErrorMessage(message: String,
                        name: String, cause: Throwable,
                        time: Long = new Date().getTime) extends LogMessage

trait VisualMessage {
  val time: Long
  val task: String
  val message: String
}

case class ProgressMessage(task: String,
                           message: String,
                           progress: Float,
                           time: Long = new Date().getTime) extends VisualMessage
case class StartTaskMessage(task: String,
                            message: String,
                            time: Long = new Date().getTime) extends VisualMessage
case class EndTaskMessage(task: String,
                          message: String,
                          time: Long = new Date().getTime) extends VisualMessage
case class DisplayMessage(task: String,
                          message: String,
                          time: Long = new Date().getTime) extends VisualMessage

trait AppMessage {
  val name: String
  val key: String
  val value: Any //message needs to be serializable
}

case class UpdateMessage(name: String, key: String, value: Any) extends AppMessage

case class ChannelMessageLogger(appName: String = "", appCtx: Option[ApplicationContext]) {

  def logInfo(message: String) {
    sendMessage(InfoMessage(message, appName))
  }

  def logError(message: String, e: Throwable) {
    sendMessage(ErrorMessage(message, appName, e))
  }

  def logWarn(message: String) {
    sendMessage(WarnMessage(message, appName))
  }

  def logDebug(message: String) {
    sendMessage(DebugMessage(message, appName))
  }

  def sendVisualMessage(message: VisualMessage) {
    sendMessage(message)
  }

  def sendUpdateMessage(key: String, value: Any) {
    sendMessage(UpdateMessage(appName, key, value))
  }

  private def sendMessage(message: Any) {

    if (appCtx.isDefined) {
      val sc = appCtx.map(_.sparkCtx)
      val messenger = appCtx.map(_.messenger)
      if (sc.isDefined) {
        messenger.map(m =>m.sendMessage(sc.get, message))
      } else
        printlnMessage(message)
    } else
      printlnMessage(message)
  }

  def printlnMessage(message: Any): Unit = {
    message match {
      case err @ ErrorMessage(msg, name, cause, time) =>
        Console.err.println(err.toString)
        cause.printStackTrace(Console.err)
      case _ =>
        println(message.toString)
    }
  }

}
