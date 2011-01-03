package spark

import scala.util.Sorting._

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
    receptionStat: ReceptionStats): Unit
}

/**
 * Helper class to send back reception stats from the reducer
 */
case class ReceptionStats(val bytesReceived: Int, val timeSpent: Int, 
  serverSplitIndex: Int) { }

/**
 * A simple ShuffleTrackerStrategy that tries to balance the total number of 
 * connections created for each mapper.
 */
class BalanceConnectionsShuffleTrackerStrategy
extends ShuffleTrackerStrategy with Logging {
  private var numSources = -1
  private var outputLocs: Array[SplitInfo] = null
  private var curConnectionsPerLoc: Array[Int] = null
  private var totalConnectionsPerLoc: Array[Int] = null
  
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
    }
  
    return splitIndex
  }
  
  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    receptionStat: ReceptionStats): Unit = synchronized {
    // Decrease number of active connections
    curConnectionsPerLoc(receptionStat.serverSplitIndex) = 
      curConnectionsPerLoc(receptionStat.serverSplitIndex) - 1

    assert(curConnectionsPerLoc(receptionStat.serverSplitIndex) >= 0)
  }
}

/**
 * Shuffle tracker strategy that tries to balance the percentage of blocks 
 * remaining for each reducer
 */
class BalanceRemainingShuffleTrackerStrategy
extends ShuffleTrackerStrategy with Logging {
  // Number of mappers
  private var numMappers = -1
  // Number of reducers
  private var numReducers = -1
  private var outputLocs: Array[SplitInfo] = null
  
  // Data structures from reducers' perspectives
  private var totalBlocksPerInputSplit: Array[Array[Int]] = null
  private var hasBlocksPerInputSplit: Array[Array[Int]] = null

  // Stored in bytes per millisecond
  private var speedPerInputSplit: Array[Array[Int]] = null
  
  private var curConnectionsPerLoc: Array[Int] = null
  private var totalConnectionsPerLoc: Array[Int] = null

  // The order of elements in the outputLocs (splitIndex) is used to pass 
  // information back and forth between the tracker, mappers, and reducers
  def initialize(outputLocs_ : Array[SplitInfo]): Unit = {
    outputLocs = outputLocs_

    numMappers = outputLocs.size

    // All the outputLocs have totalBlocksPerOutputSplit of same size
    numReducers = outputLocs(0).totalBlocksPerOutputSplit.size
    
    // Now initialize the data structures
    totalBlocksPerInputSplit = Array.tabulate(numReducers, numMappers)((i,j) => 
      outputLocs(j).totalBlocksPerOutputSplit(i))
    hasBlocksPerInputSplit = Array.tabulate(numReducers, numMappers)((_,_) => 0)

    // Initialize to -1
    speedPerInputSplit = Array.tabulate(numReducers, numMappers)((_,_) => -1)

    curConnectionsPerLoc = Array.tabulate(numMappers)(_ => 0)
    totalConnectionsPerLoc = Array.tabulate(numMappers)(_ => 0)
  }
  
  def selectSplitAndAddReducer(reducerSplitInfo: SplitInfo): Int = synchronized {
    var splitIndex = -1

    // Estimate time remaining to finish receiving for all reducer/mapper pairs
    var individualEstimates = Array.tabulate(numReducers, numMappers)((i,j) => 
      (totalBlocksPerInputSplit(i)(j) - hasBlocksPerInputSplit(i)(j)) * 
      Shuffle.BlockSize / 
      speedPerInputSplit(i)(j))

    println("reducerSplitInfo = " + reducerSplitInfo.splitId)
      
    for (i <- 0 until numReducers) {
      for (j <- 0 until numMappers) {
        print(individualEstimates(i)(j) + " ")
      }
      println("")
    }

    // Estimate time remaining to finish receiving for each reducer
    var completionEstimates = Array.tabulate(numReducers)(
      individualEstimates(_).foldLeft(Int.MinValue)(Math.max(_,_)))

    for (i <- 0 until numReducers) {
      print(completionEstimates(i) + " ")
    }
    println("")

    // Check if all individualEstimates entries have non-zero values
    var estimationComplete = true
    for (i <- 0 until numReducers; j <- 0 until numMappers) {
      if (individualEstimates(i)(j) < 0) {
        estimationComplete = false
      }
    }  
      
    // Take this reducers estimate out
    val myCompletionEstimate = completionEstimates(reducerSplitInfo.splitId)

    // Sort everyone's time
    quickSort(completionEstimates)

    // If this reducer is going to complete F times faster than the 2nd 
    // fastest one just block this one for a while
    // TODO: Must be able to support group division instead of singling one out
    // TODO: Must have a endGame fraction
    if (estimationComplete && numReducers > 1 && 
        Shuffle.ThrottleFraction * myCompletionEstimate < 
          completionEstimates(1)) {
        splitIndex = -1
        println("Throttling reducer-" + reducerSplitInfo.splitId)
    } else {
      var minConnections = Int.MaxValue
      for (i <- 0 until numMappers) {
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
    }
    
    if (splitIndex != -1) {
      curConnectionsPerLoc(splitIndex) = curConnectionsPerLoc(splitIndex) + 1
      totalConnectionsPerLoc(splitIndex) = 
        totalConnectionsPerLoc(splitIndex) + 1
    }
    
    return splitIndex
  }
  
  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    receptionStat: ReceptionStats): Unit = synchronized {
    // Update hasBlocksPerInputSplit for reducerSplitInfo
    hasBlocksPerInputSplit(reducerSplitInfo.splitId) = 
      reducerSplitInfo.hasBlocksPerInputSplit
      
    // Store the last known speed. Add 1 to avoid divide-by-zero.
    // TODO: We are forgetting the old speed. Can use averaging at some point.
    speedPerInputSplit(reducerSplitInfo.splitId)(receptionStat.serverSplitIndex) = 
      receptionStat.bytesReceived / (receptionStat.timeSpent + 1)

    // Update current connections to the mapper 
    curConnectionsPerLoc(receptionStat.serverSplitIndex) = 
      curConnectionsPerLoc(receptionStat.serverSplitIndex) - 1

    assert(curConnectionsPerLoc(receptionStat.serverSplitIndex) >= 0)
  }
}
