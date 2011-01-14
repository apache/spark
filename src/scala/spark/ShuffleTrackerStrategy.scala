package spark

import java.util.Random

import scala.util.Sorting._

/**
 * A trait for implementing tracker strategies for the shuffle system.
 */
trait ShuffleTrackerStrategy {
  // Initialize
  def initialize(outputLocs_ : Array[SplitInfo]): Unit
  
  // Select a split and send it back
  def selectSplit(reducerSplitInfo: SplitInfo): Int
  
  // Update internal stats if things could be sent back successfully
  def AddReducerToSplit(reducerSplitInfo: SplitInfo, splitIndex: Int): Unit
  
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
  
  def selectSplit(reducerSplitInfo: SplitInfo): Int = synchronized {
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
  
    return splitIndex
  }
  
  def AddReducerToSplit(reducerSplitInfo: SplitInfo, splitIndex: Int): Unit = synchronized {
    if (splitIndex != -1) {
      curConnectionsPerLoc(splitIndex) = curConnectionsPerLoc(splitIndex) + 1
      totalConnectionsPerLoc(splitIndex) = 
        totalConnectionsPerLoc(splitIndex) + 1
    }
  }

  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    receptionStat: ReceptionStats): Unit = synchronized {
    // Decrease number of active connections
    curConnectionsPerLoc(receptionStat.serverSplitIndex) = 
      curConnectionsPerLoc(receptionStat.serverSplitIndex) - 1

    // TODO: This assertion can legally fail when ShuffleClient times out while
    // waiting for tracker response and decides to go to a random server
    // assert(curConnectionsPerLoc(receptionStat.serverSplitIndex) >= 0)

    // Just in case
    if (curConnectionsPerLoc(receptionStat.serverSplitIndex) < 0) {
      curConnectionsPerLoc(receptionStat.serverSplitIndex) = 0
    }
  }
}

/**
 * A simple ShuffleTrackerStrategy that randomly selects mapper for each reducer
 */
class SelectRandomShuffleTrackerStrategy
extends ShuffleTrackerStrategy with Logging {
  private var numMappers = -1
  private var outputLocs: Array[SplitInfo] = null
  
  private var ranGen = new Random
  
  // The order of elements in the outputLocs (splitIndex) is used to pass 
  // information back and forth between the tracker, mappers, and reducers
  def initialize(outputLocs_ : Array[SplitInfo]): Unit = {
    outputLocs = outputLocs_
    numMappers = outputLocs.size
  }
  
  def selectSplit(reducerSplitInfo: SplitInfo): Int = synchronized {
    var splitIndex = -1
    
    do {
      splitIndex = ranGen.nextInt(numMappers)
    } while (reducerSplitInfo.hasSplitsBitVector.get(splitIndex))
    
    return splitIndex
  }
  
  def AddReducerToSplit(reducerSplitInfo: SplitInfo, splitIndex: Int): Unit = synchronized {
  }

  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    receptionStat: ReceptionStats): Unit = synchronized {
    // TODO: This assertion can legally fail when ShuffleClient times out while
    // waiting for tracker response and decides to go to a random server
    // assert(curConnectionsPerLoc(receptionStat.serverSplitIndex) >= 0)
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
  private var speedPerInputSplit: Array[Array[Double]] = null
  
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
    speedPerInputSplit = Array.tabulate(numReducers, numMappers)((_,_) => -1.0)

    curConnectionsPerLoc = Array.tabulate(numMappers)(_ => 0)
    totalConnectionsPerLoc = Array.tabulate(numMappers)(_ => 0)
  }
  
  def selectSplit(reducerSplitInfo: SplitInfo): Int = synchronized {
    var splitIndex = -1

    // Estimate time remaining to finish receiving for all reducer/mapper pairs
    // If speed is unknown or zero then make it 1 to give a large estimate
    var individualEstimates = Array.tabulate(numReducers, numMappers)((_,_) => 0.0)
    for (i <- 0 until numReducers; j <- 0 until numMappers) {
      var blocksRemaining = totalBlocksPerInputSplit(i)(j) - 
        hasBlocksPerInputSplit(i)(j)
      assert(blocksRemaining >= 0)
      
      individualEstimates(i)(j) = 1.0 * blocksRemaining * Shuffle.BlockSize / 
        { if (speedPerInputSplit(i)(j) <= 0.0) 1.0 else speedPerInputSplit(i)(j) }
    }
    
    // Check if all speedPerInputSplit entries have non-zero values
    var estimationComplete = true
    for (i <- 0 until numReducers; j <- 0 until numMappers) {
      if (speedPerInputSplit(i)(j) < 0.0) {
        estimationComplete = false
      }
    }  

    // Mark mappers where this reducer is too fast 
    var throttleFromMapper = Array.tabulate(numMappers)(_ => false)
    
    for (i <- 0 until numMappers) {
      var estimatesFromAMapper = 
        Array.tabulate(numReducers)(j => individualEstimates(j)(i))
        
      val estimateOfThisReducer = estimatesFromAMapper(reducerSplitInfo.splitId)
      
      // Only care if this reducer yet has something to receive from this mapper
      if (estimateOfThisReducer > 0) {
        // Sort the estimated times
        quickSort(estimatesFromAMapper)
        
        // Find a Shuffle.ThrottleFraction amount of gap
        var gapIndex = -1
        for (i <- 0 until numReducers - 1) {
          if (gapIndex == -1 && estimatesFromAMapper(i) > 0 && 
              (Shuffle.ThrottleFraction * estimatesFromAMapper(i) < 
              estimatesFromAMapper(i + 1))) {
            gapIndex = i
          }
          
          assert (estimatesFromAMapper(i) <= estimatesFromAMapper(i + 1))
        }
        
        // Keep track of how many have completed
        var numComplete = estimatesFromAMapper.findIndexOf(i => (i > 0)) 
        if (numComplete == -1) {
          numComplete = numReducers
        }

        // TODO: Pick a configurable parameter
        if (gapIndex != -1 && (1.0 * (gapIndex - numComplete + 1) < 0.1 * Shuffle.ThrottleFraction * (numReducers - numComplete)) && 
            estimateOfThisReducer <= estimatesFromAMapper(gapIndex)) {
          throttleFromMapper(i) = true
          logInfo("Throttling R-%d at M-%d with %d and cut-off %d at %d".format(reducerSplitInfo.splitId, i, estimateOfThisReducer, estimatesFromAMapper(gapIndex + 1), gapIndex))
//          for (i <- 0 until numReducers) {
//            print(estimatesFromAMapper(i) + " ")
//          }
//          println("")
        }
      } else {
        throttleFromMapper(i) = true
      }
    }
    
    var minConnections = Int.MaxValue
    for (i <- 0 until numMappers) {
      // TODO: Use of MaxRxConnections instead of MaxTxConnections is 
      // intentional here. MaxTxConnections is per machine whereas 
      // MaxRxConnections is per mapper/reducer. Will have to find a better way.
      if (curConnectionsPerLoc(i) < Shuffle.MaxRxConnections &&
          totalConnectionsPerLoc(i) < minConnections && 
          !reducerSplitInfo.hasSplitsBitVector.get(i) &&
          !throttleFromMapper(i)) {
        minConnections = totalConnectionsPerLoc(i)
        splitIndex = i
      }
    }
    
    return splitIndex
  }
  
  def AddReducerToSplit(reducerSplitInfo: SplitInfo, splitIndex: Int): Unit = synchronized {
    if (splitIndex != -1) {
      curConnectionsPerLoc(splitIndex) += 1
      totalConnectionsPerLoc(splitIndex) += 1
    }
  }

  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    receptionStat: ReceptionStats): Unit = synchronized {
    // Update hasBlocksPerInputSplit for reducerSplitInfo
    hasBlocksPerInputSplit(reducerSplitInfo.splitId) = 
      reducerSplitInfo.hasBlocksPerInputSplit
      
    // Store the last known speed. Add 1 to avoid divide-by-zero. Ignore 0 bytes
    // TODO: We are forgetting the old speed. Can use averaging at some point.
    if (receptionStat.bytesReceived > 0) {
      speedPerInputSplit(reducerSplitInfo.splitId)(receptionStat.serverSplitIndex) = 
        1.0 * receptionStat.bytesReceived / (receptionStat.timeSpent + 1.0)
    }
    
    logInfo("%d received %d bytes in %d millis".format(reducerSplitInfo.splitId, receptionStat.bytesReceived, receptionStat.timeSpent))

    // Update current connections to the mapper 
    curConnectionsPerLoc(receptionStat.serverSplitIndex) -= 1

    // TODO: This assertion can legally fail when ShuffleClient times out while
    // waiting for tracker response and decides to go to a random server
    // assert(curConnectionsPerLoc(receptionStat.serverSplitIndex) >= 0)
    
    // Just in case
    if (curConnectionsPerLoc(receptionStat.serverSplitIndex) < 0) {
      curConnectionsPerLoc(receptionStat.serverSplitIndex) = 0
    }
  }
}

/**
 * Shuffle tracker strategy that allows reducers to create receiving threads
 * depending on their estimated time remaining
 */
class LimitConnectionsShuffleTrackerStrategy
extends ShuffleTrackerStrategy with Logging {
  // Number of mappers
  private var numMappers = -1
  // Number of reducers
  private var numReducers = -1
  private var outputLocs: Array[SplitInfo] = null
  
  private var ranGen = new Random
  
  // Data structures from reducers' perspectives
  private var totalBlocksPerInputSplit: Array[Array[Int]] = null
  private var hasBlocksPerInputSplit: Array[Array[Int]] = null

  // Stored in bytes per millisecond
  private var speedPerInputSplit: Array[Array[Double]] = null
  
  private var curConnectionsPerReducer: Array[Int] = null
  private var maxConnectionsPerReducer: Array[Int] = null

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
    speedPerInputSplit = Array.tabulate(numReducers, numMappers)((_,_) => -1.0)

    curConnectionsPerReducer = Array.tabulate(numReducers)(_ => 0)
    maxConnectionsPerReducer = Array.tabulate(numReducers)(_ => Shuffle.MaxRxConnections)
  }
  
  def selectSplit(reducerSplitInfo: SplitInfo): Int = synchronized {
    var splitIndex = -1

    // Estimate time remaining to finish receiving for all reducer/mapper pairs
    // If speed is unknown or zero then make it 1 to give a large estimate
    var individualEstimates = Array.tabulate(numReducers, numMappers)((_,_) => 0.0)
    for (i <- 0 until numReducers; j <- 0 until numMappers) {
      var blocksRemaining = totalBlocksPerInputSplit(i)(j) - 
        hasBlocksPerInputSplit(i)(j)
      assert(blocksRemaining >= 0)
      
      individualEstimates(i)(j) = 1.0 * blocksRemaining * Shuffle.BlockSize / 
        { if (speedPerInputSplit(i)(j) <= 0.0) 1.0 else speedPerInputSplit(i)(j) }
    }
    
    // Check if all speedPerInputSplit entries have non-zero values
    var estimationComplete = true
    for (i <- 0 until numReducers; j <- 0 until numMappers) {
      if (speedPerInputSplit(i)(j) < 0.0) {
        estimationComplete = false
      }
    }
    
    if (estimationComplete) {
      // Estimate time remaining to finish receiving for each reducer
      var completionEstimates = Array.tabulate(numReducers)(
        individualEstimates(_).foldLeft(Double.MinValue)(Math.max(_,_)))
        
      val fastestEstimate = 
        completionEstimates.foldLeft(Double.MaxValue)(Math.min(_,_))
      val slowestEstimate = 
        completionEstimates.foldLeft(Double.MinValue)(Math.max(_,_))
      
      // Set maxConnectionsPerReducer for all reducers proportional to their
      // estimated time remaining with slowestEstimate reducer having the max
      for (i <- 0 until numReducers) {
        maxConnectionsPerReducer(i) = 
          ((completionEstimates(i) / slowestEstimate) * Shuffle.MaxRxConnections).toInt
      }
    }

    // Send back a splitIndex if this reducer is within its limit
    if (curConnectionsPerReducer(reducerSplitInfo.splitId) < 
      maxConnectionsPerReducer(reducerSplitInfo.splitId)) {
    
      do {
        splitIndex = ranGen.nextInt(numMappers)
      } while (reducerSplitInfo.hasSplitsBitVector.get(splitIndex))   
    }
    
    return splitIndex
  }
  
  def AddReducerToSplit(reducerSplitInfo: SplitInfo, splitIndex: Int): Unit = synchronized {
    if (splitIndex != -1) {
      curConnectionsPerReducer(reducerSplitInfo.splitId) += 1
    }
  }

  def deleteReducerFrom(reducerSplitInfo: SplitInfo, 
    receptionStat: ReceptionStats): Unit = synchronized {
    // Update hasBlocksPerInputSplit for reducerSplitInfo
    hasBlocksPerInputSplit(reducerSplitInfo.splitId) = 
      reducerSplitInfo.hasBlocksPerInputSplit
      
    // Store the last known speed. Add 1 to avoid divide-by-zero. Ignore 0 bytes
    // TODO: We are forgetting the old speed. Can use averaging at some point.
    if (receptionStat.bytesReceived > 0) {
      speedPerInputSplit(reducerSplitInfo.splitId)(receptionStat.serverSplitIndex) = 
        1.0 * receptionStat.bytesReceived / (receptionStat.timeSpent + 1.0)
    }
    
    logInfo("%d received %d bytes in %d millis".format(reducerSplitInfo.splitId, receptionStat.bytesReceived, receptionStat.timeSpent))
    
    // Update current threads by this reducer
    curConnectionsPerReducer(reducerSplitInfo.splitId) -= 1

    // TODO: This assertion can legally fail when ShuffleClient times out while
    // waiting for tracker response and decides to go to a random server
    // assert(curConnectionsPerLoc(receptionStat.serverSplitIndex) >= 0)
    
    // Just in case
    if (curConnectionsPerReducer(reducerSplitInfo.splitId) < 0) {
      curConnectionsPerReducer(reducerSplitInfo.splitId) = 0
    }
  }
}
