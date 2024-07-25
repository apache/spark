package org.apache.spark.util

import java.text.SimpleDateFormat
import java.util.Date

import scala.sys.process._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

object HeapUtils extends Logging {

  def createSparkSubmitHeapDump(heapDumpPath: String): Option[String] = {
    // Generate timestamp.
    val timestampFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
    val currentTimestamp = timestampFormat.format(new Date())
    println("Creating heap dump")

    try {
      // Run ps command.
      val psCommand = "ps aux"
      val psOutput = psCommand.!!.split("\n")

      // Find SparkSubmit PID
      var sparkSubmitPid = psOutput
        .find(line => line.contains("SparkSubmit") && !line.contains("grep"))
        .map(_.split("\\s+")(1))

      if (sparkSubmitPid.isEmpty) {
        println(log"Process ID was not found. Failed to create heap dump.")
        return None
      }

      val pid = sparkSubmitPid.get

      // Use timestamp in the jmap file name
      val filename = s"$heapDumpPath/$currentTimestamp.jmap"
      val jmapCmd = s"jmap -dump:live,format=b,file=$filename $pid"
      val result = jmapCmd.!
      if (result != 0) {
        println(s"Failed to execute jmap command, exit code: $result")
      } else {
        log.info(s"Heap dump created: $filename")
        return Some(filename)
      }
    } catch {
      case NonFatal(e) =>
        logError(log"Failed to create heap dump.", e)
    }
    return None
  }
}
