package spark

/**
 * A trait for implementing tracker strategies for the shuffle system.
 */
trait ShuffleTrackerStrategy {
  // Initialize
  def initialize(outputLocs_ : Array[SplitInfo]): Unit
  
  // Select a split, update internal stats, and send it back
  def selectSplitAndAddReducer(reducerSplitInfo: SplitInfo): Int
  
  // A reducer is done. Update internal stats
  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    serverSplitIndex: Int): Unit
}

/**
 * A simple ShuffleTrackerStrategy that tries to balance the total number of 
 * connections created for each mapper.
 */
class BalanceConnectionsShuffleTrackerStrategy
extends ShuffleTrackerStrategy with Logging {
  var numSources = -1
  var outputLocs: Array[SplitInfo] = null
  var curConnectionsPerLoc: Array[Int] = null
  var totalConnectionsPerLoc: Array[Int] = null
  
  // The order of elements in the outputLocs (splitIndex) is used to pass 
  // information back and forth between the tracker, mappers, and reducers
  def initialize(outputLocs_ : Array[SplitInfo]): Unit = {
    outputLocs = outputLocs_
    numSources = outputLocs.size
    
    // Now initialize other data structures
    curConnectionsPerLoc = Array.tabulate(numSources)(_ => 0)
    totalConnectionsPerLoc = Array.tabulate(numSources)(_ => 0)
  }
  
  def selectSplitAndAddReducer(reducerSplitInfo: SplitInfo): Int = synchronized {
    var minConnections = Int.MaxValue
    var splitIndex = -1
    
    for (i <- 0 until numSources) {
      // TODO: Use of MaxRxConnections instead of MaxTxConnections is 
      // intentional here. MaxTxConnections is per machine whereas 
      // MaxRxConnections is per mapper/reducer. Will have to find a better way.
      if (curConnectionsPerLoc(i) < Shuffle.MaxRxConnections &&
        totalConnectionsPerLoc(i) < minConnections && 
        !reducerSplitInfo.hasSplitsBitVector.get(i)) {
        minConnections = totalConnectionsPerLoc(i)
        splitIndex = i
      }
    }
  
    if (splitIndex != -1) {
      curConnectionsPerLoc(splitIndex) = curConnectionsPerLoc(splitIndex) + 1
      totalConnectionsPerLoc(splitIndex) = 
        totalConnectionsPerLoc(splitIndex) + 1
        
      curConnectionsPerLoc.foreach { i =>
        print ("" + i + " ")
      }
      println("")
    }
  
    return splitIndex
  }
  
  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    serverSplitIndex: Int): Unit = synchronized {
    // Decrease number of active connections
    curConnectionsPerLoc(serverSplitIndex) = 
      curConnectionsPerLoc(serverSplitIndex) - 1

    assert(curConnectionsPerLoc(serverSplitIndex) >= 0)

    curConnectionsPerLoc.foreach {  i =>
      print ("" + i + " ")
    }
    println("")
  }
}
