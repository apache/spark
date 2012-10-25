package spark.util

import spark.Utils

/**
 * An extractor object for parsing JVM memory strings, such as "10g", into an Int representing
 * the number of megabytes. Supports the same formats as Utils.memoryStringToMb.
 */
private[spark] object MemoryParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(Utils.memoryStringToMb(str))
    } catch {
      case e: NumberFormatException => None
    }
  }
}
