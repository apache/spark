package spark.deploy

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Utilities used throughout the web UI.
 */
private[spark] object WebUI {
  val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  def formatDate(date: Date): String = DATE_FORMAT.format(date)

  def formatDate(timestamp: Long): String = DATE_FORMAT.format(new Date(timestamp))

  def formatDuration(milliseconds: Long): String = {
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    return "%.1f h".format(hours)
  }
}
