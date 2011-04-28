package spark.shuffle

import java.util.BitSet

/**
 * Used to keep and pass around information of output splits during shuffle
 */

@serializable
case class SplitInfo(val hostAddress: String, 
                     val listenPort: Int,
                     val splitId: Int) { 

  var hasSplits = 0
  var hasSplitsBitVector: BitSet = null
  
  // Used by mappers of dim |numOutputSplits|
  var totalBlocksPerOutputSplit: Array[Int] = null
  // Used by reducers of dim |numInputSplits|
  var hasBlocksPerInputSplit: Array[Int] = null
}

/**
 * Helper Object of SplitInfo for its constants
 */
object SplitInfo {
  // Constants for special values of listenPort
  val MappersBusy = -1

  // Other constants
  val UnusedParam = 0
}