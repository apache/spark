package org.apache.spark.sql

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.streaming.Offset

/**
 * :: DeveloperApi ::
 *
 * @param message     Message of this exception
 * @param cause       Internal cause of the exception
 * @param startOffset Starting offset (if known) of the range of data in which exception occurred
 * @param endOffset   Ending offset (if known) of the range of data in exception occurred
 */
@DeveloperApi
class QueryException private[sql](
    val message: String,
    val cause: Throwable,
    val startOffset: Option[Offset] = None,
    val endOffset: Option[Offset] = None
  ) extends Exception(message, cause) {

  /** Time when the exception occurred */
  val time: Long = System.currentTimeMillis

  override def toString(): String = {
    val causeStr =
      s"${cause.getMessage} ${cause.getStackTrace.take(10).mkString("", "\n|\t", "\n")}"
    s"""
       |$message
       |
       |=== Error ===
       |$causeStr
       |
       |=== Offset range ===
       |Start: ${startOffset.map { _.toString }.getOrElse("-")}
       |End:   ${endOffset.map { _.toString }.getOrElse("-")}
       |
       """.stripMargin
  }
}
