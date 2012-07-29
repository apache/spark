package spark.stream
import spark.Logging
import scala.collection.mutable.{ArrayBuffer, HashMap}

object TestInputBlockTracker extends Logging {
  initLogging()
  val allBlockIds = new HashMap[Time, ArrayBuffer[String]]()

  def addBlocks(intervalEndTime: Time, reference: AnyRef) {
    allBlockIds.getOrElseUpdate(intervalEndTime, new ArrayBuffer[String]()) ++= reference.asInstanceOf[Array[String]]
  }

  def setEndTime(intervalEndTime: Time) {
    try {
      val endTime = System.currentTimeMillis
      allBlockIds.get(intervalEndTime) match {
        case Some(blockIds) => {
          val numBlocks = blockIds.size
          var totalDelay = 0d
          blockIds.foreach(blockId => {
            val inputTime = getInputTime(blockId)
            val delay = (endTime - inputTime) / 1000.0
            totalDelay += delay
            logInfo("End-to-end delay for block " + blockId + " is " + delay + " s")
          })
          logInfo("Average end-to-end delay for time " + intervalEndTime + " is " + (totalDelay / numBlocks) + " s")
          allBlockIds -= intervalEndTime
        }
        case None => throw new Exception("Unexpected")
      }
    } catch {
      case e: Exception => logError(e.toString)
    }
  }

  def getInputTime(blockId: String): Long = {
    val parts = blockId.split("-")
    /*logInfo(blockId + " -> " + parts(4)) */
    parts(4).toLong
  }
}

