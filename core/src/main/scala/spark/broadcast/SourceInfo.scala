package spark.broadcast

import java.util.BitSet

import spark._

/**
 * Used to keep and pass around information of peers involved in a broadcast
 * 
 * CHANGED: Keep track of the blockSize for THIS broadcast variable.
 * Broadcast.BlockSize is expected to be updated across different broadcasts
 */
case class SourceInfo (hostAddress: String,
                       listenPort: Int,
                       totalBlocks: Int = SourceInfo.UnusedParam,
                       totalBytes: Int = SourceInfo.UnusedParam,
                       blockSize: Int = Broadcast.BlockSize)
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