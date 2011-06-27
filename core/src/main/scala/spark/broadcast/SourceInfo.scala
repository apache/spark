package spark.broadcast

import java.util.BitSet

import spark._

/**
 * Used to keep and pass around information of peers involved in a broadcast
 * 
 * CHANGED: Keep track of the blockSize for THIS broadcast variable.
 * Broadcast.BlockSize is expected to be updated across different broadcasts
 */
@serializable
case class SourceInfo (val hostAddress: String,
                       val listenPort: Int,
                       val totalBlocks: Int = SourceInfo.UnusedParam,
                       val totalBytes: Int = SourceInfo.UnusedParam,
                       val blockSize: Int = Broadcast.BlockSize)
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
object SourceInfo {
  // Constants for special values of listenPort
  val TxNotStartedRetry = -1
  val TxOverGoToHDFS = 0
  // Other constants
  val StopBroadcast = -2
  val UnusedParam = 0
}