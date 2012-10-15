package spark.broadcast

import java.util.BitSet

import spark._

/**
 * Used to keep and pass around information of peers involved in a broadcast
 */
private[spark] case class SourceInfo (hostAddress: String,
                       listenPort: Int,
                       totalBlocks: Int = SourceInfo.UnusedParam,
                       totalBytes: Int = SourceInfo.UnusedParam)
extends Comparable[SourceInfo] with Logging {

  var currentLeechers = 0
  var receptionFailed = false

  var hasBlocks = 0
  var hasBlocksBitVector: BitSet = new BitSet (totalBlocks)

  // Ascending sort based on leecher count
  def compareTo (o: SourceInfo): Int = (currentLeechers - o.currentLeechers)
}

/**
 * Helper Object of SourceInfo for its constants
 */
private[spark] object SourceInfo {
  // Broadcast has not started yet! Should never happen.
  val TxNotStartedRetry = -1
  // Broadcast has already finished. Try default mechanism.
  val TxOverGoToDefault = -3
  // Other constants
  val StopBroadcast = -2
  val UnusedParam = 0
}
