package spark.broadcast

import java.io._
import java.net._
import java.util.{Comparator, Random, UUID}

import scala.collection.mutable.{ListBuffer, Map, Set}
import scala.math

import spark._
import spark.storage.StorageLevel

private[spark] class TreeBroadcast[T](@transient var value_ : T, isLocal: Boolean, id: Long)
extends Broadcast[T](id) with Logging with Serializable {

  def value = value_

  def blockId = "broadcast_" + id

  MultiTracker.synchronized {
    SparkEnv.get.blockManager.putSingle(blockId, value_, StorageLevel.MEMORY_AND_DISK, false)
  }

  @transient var arrayOfBlocks: Array[BroadcastBlock] = null
  @transient var totalBytes = -1
  @transient var totalBlocks = -1
  @transient var hasBlocks = 0

  @transient var listenPortLock = new Object
  @transient var guidePortLock = new Object
  @transient var totalBlocksLock = new Object
  @transient var hasBlocksLock = new Object

  @transient var listOfSources = ListBuffer[SourceInfo]()

  @transient var serveMR: ServeMultipleRequests = null
  @transient var guideMR: GuideMultipleRequests = null

  @transient var hostAddress = Utils.localIpAddress
  @transient var listenPort = -1
  @transient var guidePort = -1

  @transient var stopBroadcast = false

  // Must call this after all the variables have been created/initialized
  if (!isLocal) {
    sendBroadcast()
  }

  def sendBroadcast() {
    logInfo("Local host address: " + hostAddress)

    // Create a variableInfo object and store it in valueInfos
    var variableInfo = MultiTracker.blockifyObject(value_)

    // Prepare the value being broadcasted
    arrayOfBlocks = variableInfo.arrayOfBlocks
    totalBytes = variableInfo.totalBytes
    totalBlocks = variableInfo.totalBlocks
    hasBlocks = variableInfo.totalBlocks

    guideMR = new GuideMultipleRequests
    guideMR.setDaemon(true)
    guideMR.start()
    logInfo("GuideMultipleRequests started...")

    // Must always come AFTER guideMR is created
    while (guidePort == -1) {
      guidePortLock.synchronized { guidePortLock.wait() }
    }

    serveMR = new ServeMultipleRequests
    serveMR.setDaemon(true)
    serveMR.start()
    logInfo("ServeMultipleRequests started...")

    // Must always come AFTER serveMR is created
    while (listenPort == -1) {
      listenPortLock.synchronized { listenPortLock.wait() }
    }

    // Must always come AFTER listenPort is created
    val masterSource =
      SourceInfo(hostAddress, listenPort, totalBlocks, totalBytes)
    listOfSources += masterSource

    // Register with the Tracker
    MultiTracker.registerBroadcast(id,
      SourceInfo(hostAddress, guidePort, totalBlocks, totalBytes))
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    MultiTracker.synchronized {
      SparkEnv.get.blockManager.getSingle(blockId) match {
        case Some(x) =>
          value_ = x.asInstanceOf[T]

        case None =>
          logInfo("Started reading broadcast variable " + id)
          // Initializing everything because Driver will only send null/0 values
          // Only the 1st worker in a node can be here. Others will get from cache
          initializeWorkerVariables()

          logInfo("Local host address: " + hostAddress)

          serveMR = new ServeMultipleRequests
          serveMR.setDaemon(true)
          serveMR.start()
          logInfo("ServeMultipleRequests started...")

          val start = System.nanoTime

          val receptionSucceeded = receiveBroadcast(id)
          if (receptionSucceeded) {
            value_ = MultiTracker.unBlockifyObject[T](arrayOfBlocks, totalBytes, totalBlocks)
            SparkEnv.get.blockManager.putSingle(
              blockId, value_, StorageLevel.MEMORY_AND_DISK, false)
          }  else {
            logError("Reading broadcast variable " + id + " failed")
          }

          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
      }
    }
  }

  private def initializeWorkerVariables() {
    arrayOfBlocks = null
    totalBytes = -1
    totalBlocks = -1
    hasBlocks = 0

    listenPortLock = new Object
    totalBlocksLock = new Object
    hasBlocksLock = new Object

    serveMR =  null

    hostAddress = Utils.localIpAddress
    listenPort = -1

    stopBroadcast = false
  }

  def receiveBroadcast(variableID: Long): Boolean = {
    val gInfo = MultiTracker.getGuideInfo(variableID)
    
    if (gInfo.listenPort == SourceInfo.TxOverGoToDefault) {
      return false
    }

    // Wait until hostAddress and listenPort are created by the
    // ServeMultipleRequests thread
    while (listenPort == -1) {
      listenPortLock.synchronized { listenPortLock.wait() }
    }

    var clientSocketToDriver: Socket = null
    var oosDriver: ObjectOutputStream = null
    var oisDriver: ObjectInputStream = null

    // Connect and receive broadcast from the specified source, retrying the
    // specified number of times in case of failures
    var retriesLeft = MultiTracker.MaxRetryCount
    do {
      // Connect to Driver and send this worker's Information
      clientSocketToDriver = new Socket(MultiTracker.DriverHostAddress, gInfo.listenPort)
      oosDriver = new ObjectOutputStream(clientSocketToDriver.getOutputStream)
      oosDriver.flush()
      oisDriver = new ObjectInputStream(clientSocketToDriver.getInputStream)

      logDebug("Connected to Driver's guiding object")

      // Send local source information
      oosDriver.writeObject(SourceInfo(hostAddress, listenPort))
      oosDriver.flush()

      // Receive source information from Driver
      var sourceInfo = oisDriver.readObject.asInstanceOf[SourceInfo]
      totalBlocks = sourceInfo.totalBlocks
      arrayOfBlocks = new Array[BroadcastBlock](totalBlocks)
      totalBlocksLock.synchronized { totalBlocksLock.notifyAll() }
      totalBytes = sourceInfo.totalBytes

      logDebug("Received SourceInfo from Driver:" + sourceInfo + " My Port: " + listenPort)

      val start = System.nanoTime
      val receptionSucceeded = receiveSingleTransmission(sourceInfo)
      val time = (System.nanoTime - start) / 1e9

      // Updating some statistics in sourceInfo. Driver will be using them later
      if (!receptionSucceeded) {
        sourceInfo.receptionFailed = true
      }

      // Send back statistics to the Driver
      oosDriver.writeObject(sourceInfo)

      if (oisDriver != null) {
        oisDriver.close()
      }
      if (oosDriver != null) {
        oosDriver.close()
      }
      if (clientSocketToDriver != null) {
        clientSocketToDriver.close()
      }

      retriesLeft -= 1
    } while (retriesLeft > 0 && hasBlocks < totalBlocks)

    return (hasBlocks == totalBlocks)
  }

  /**
   * Tries to receive broadcast from the source and returns Boolean status.
   * This might be called multiple times to retry a defined number of times.
   */
  private def receiveSingleTransmission(sourceInfo: SourceInfo): Boolean = {
    var clientSocketToSource: Socket = null
    var oosSource: ObjectOutputStream = null
    var oisSource: ObjectInputStream = null

    var receptionSucceeded = false
    try {
      // Connect to the source to get the object itself
      clientSocketToSource = new Socket(sourceInfo.hostAddress, sourceInfo.listenPort)
      oosSource = new ObjectOutputStream(clientSocketToSource.getOutputStream)
      oosSource.flush()
      oisSource = new ObjectInputStream(clientSocketToSource.getInputStream)

      logDebug("Inside receiveSingleTransmission")
      logDebug("totalBlocks: "+ totalBlocks + " " + "hasBlocks: " + hasBlocks)

      // Send the range
      oosSource.writeObject((hasBlocks, totalBlocks))
      oosSource.flush()

      for (i <- hasBlocks until totalBlocks) {
        val recvStartTime = System.currentTimeMillis
        val bcBlock = oisSource.readObject.asInstanceOf[BroadcastBlock]
        val receptionTime = (System.currentTimeMillis - recvStartTime)

        logDebug("Received block: " + bcBlock.blockID + " from " + sourceInfo + " in " + receptionTime + " millis.")

        arrayOfBlocks(hasBlocks) = bcBlock
        hasBlocks += 1
        
        // Set to true if at least one block is received
        receptionSucceeded = true
        hasBlocksLock.synchronized { hasBlocksLock.notifyAll() }
      }
    } catch {
      case e: Exception => logError("receiveSingleTransmission had a " + e)
    } finally {
      if (oisSource != null) {
        oisSource.close()
      }
      if (oosSource != null) {
        oosSource.close()
      }
      if (clientSocketToSource != null) {
        clientSocketToSource.close()
      }
    }

    return receptionSucceeded
  }

  class GuideMultipleRequests
  extends Thread with Logging {
    // Keep track of sources that have completed reception
    private var setOfCompletedSources = Set[SourceInfo]()

    override def run() {
      var threadPool = Utils.newDaemonCachedThreadPool()
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket(0)
      guidePort = serverSocket.getLocalPort
      logInfo("GuideMultipleRequests => " + serverSocket + " " + guidePort)

      guidePortLock.synchronized { guidePortLock.notifyAll() }

      try {
        while (!stopBroadcast) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(MultiTracker.ServerSocketTimeout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => {
              // Stop broadcast if at least one worker has connected and
              // everyone connected so far are done. Comparing with
              // listOfSources.size - 1, because it includes the Guide itself
              listOfSources.synchronized {
                setOfCompletedSources.synchronized {
                  if (listOfSources.size > 1 &&
                    setOfCompletedSources.size == listOfSources.size - 1) {
                    stopBroadcast = true
                    logInfo("GuideMultipleRequests Timeout. stopBroadcast == true.")
                  }
                }
              }
            }
          }
          if (clientSocket != null) {
            logDebug("Guide: Accepted new client connection: " + clientSocket)
            try {
              threadPool.execute(new GuideSingleRequest(clientSocket))
            } catch {
              // In failure, close() the socket here; else, the thread will close() it
              case ioe: IOException => clientSocket.close()
            }
          }
        }

        logInfo("Sending stopBroadcast notifications...")
        sendStopBroadcastNotifications

        MultiTracker.unregisterBroadcast(id)
      } finally {
        if (serverSocket != null) {
          logInfo("GuideMultipleRequests now stopping...")
          serverSocket.close()
        }
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }

    private def sendStopBroadcastNotifications() {
      listOfSources.synchronized {
        var listIter = listOfSources.iterator
        while (listIter.hasNext) {
          var sourceInfo = listIter.next

          var guideSocketToSource: Socket = null
          var gosSource: ObjectOutputStream = null
          var gisSource: ObjectInputStream = null

          try {
            // Connect to the source
            guideSocketToSource = new Socket(sourceInfo.hostAddress, sourceInfo.listenPort)
            gosSource = new ObjectOutputStream(guideSocketToSource.getOutputStream)
            gosSource.flush()
            gisSource = new ObjectInputStream(guideSocketToSource.getInputStream)

            // Send stopBroadcast signal
            gosSource.writeObject((SourceInfo.StopBroadcast, SourceInfo.StopBroadcast))
            gosSource.flush()
          } catch {
            case e: Exception => {
              logError("sendStopBroadcastNotifications had a " + e)
            }
          } finally {
            if (gisSource != null) {
              gisSource.close()
            }
            if (gosSource != null) {
              gosSource.close()
            }
            if (guideSocketToSource != null) {
              guideSocketToSource.close()
            }
          }
        }
      }
    }

    class GuideSingleRequest(val clientSocket: Socket)
    extends Thread with Logging {
      private val oos = new ObjectOutputStream(clientSocket.getOutputStream)
      oos.flush()
      private val ois = new ObjectInputStream(clientSocket.getInputStream)

      private var selectedSourceInfo: SourceInfo = null
      private var thisWorkerInfo:SourceInfo = null

      override def run() {
        try {
          logInfo("new GuideSingleRequest is running")
          // Connecting worker is sending in its hostAddress and listenPort it will
          // be listening to. Other fields are invalid (SourceInfo.UnusedParam)
          var sourceInfo = ois.readObject.asInstanceOf[SourceInfo]

          listOfSources.synchronized {
            // Select a suitable source and send it back to the worker
            selectedSourceInfo = selectSuitableSource(sourceInfo)
            logDebug("Sending selectedSourceInfo: " + selectedSourceInfo)
            oos.writeObject(selectedSourceInfo)
            oos.flush()

            // Add this new (if it can finish) source to the list of sources
            thisWorkerInfo = SourceInfo(sourceInfo.hostAddress,
              sourceInfo.listenPort, totalBlocks, totalBytes)
            logDebug("Adding possible new source to listOfSources: " + thisWorkerInfo)
            listOfSources += thisWorkerInfo
          }

          // Wait till the whole transfer is done. Then receive and update source
          // statistics in listOfSources
          sourceInfo = ois.readObject.asInstanceOf[SourceInfo]

          listOfSources.synchronized {
            // This should work since SourceInfo is a case class
            assert(listOfSources.contains(selectedSourceInfo))

            // Remove first 
            // (Currently removing a source based on just one failure notification!)
            listOfSources = listOfSources - selectedSourceInfo

            // Update sourceInfo and put it back in, IF reception succeeded
            if (!sourceInfo.receptionFailed) {
              // Add thisWorkerInfo to sources that have completed reception
              setOfCompletedSources.synchronized {
                setOfCompletedSources += thisWorkerInfo
              }

              // Update leecher count and put it back in 
              selectedSourceInfo.currentLeechers -= 1
              listOfSources += selectedSourceInfo
            }
          }
        } catch {
          case e: Exception => {
            // Remove failed worker from listOfSources and update leecherCount of
            // corresponding source worker
            listOfSources.synchronized {
              if (selectedSourceInfo != null) {
                // Remove first
                listOfSources = listOfSources - selectedSourceInfo
                // Update leecher count and put it back in
                selectedSourceInfo.currentLeechers -= 1
                listOfSources += selectedSourceInfo
              }

              // Remove thisWorkerInfo
              if (listOfSources != null) {
                listOfSources = listOfSources - thisWorkerInfo
              }
            }
          }
        } finally {
          logInfo("GuideSingleRequest is closing streams and sockets")
          ois.close()
          oos.close()
          clientSocket.close()
        }
      }

      // Assuming the caller to have a synchronized block on listOfSources
      // Select one with the most leechers. This will level-wise fill the tree
      private def selectSuitableSource(skipSourceInfo: SourceInfo): SourceInfo = {
        var maxLeechers = -1
        var selectedSource: SourceInfo = null

        listOfSources.foreach { source =>
          if ((source.hostAddress != skipSourceInfo.hostAddress || 
               source.listenPort != skipSourceInfo.listenPort) && 
            source.currentLeechers < MultiTracker.MaxDegree &&
            source.currentLeechers > maxLeechers) {
              selectedSource = source
              maxLeechers = source.currentLeechers
            }
        }

        // Update leecher count
        selectedSource.currentLeechers += 1
        return selectedSource
      }
    }
  }

  class ServeMultipleRequests
  extends Thread with Logging {
    
    var threadPool = Utils.newDaemonCachedThreadPool()
    
    override def run() {      
      var serverSocket = new ServerSocket(0)
      listenPort = serverSocket.getLocalPort
      
      logInfo("ServeMultipleRequests started with " + serverSocket)

      listenPortLock.synchronized { listenPortLock.notifyAll() }

      try {
        while (!stopBroadcast) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(MultiTracker.ServerSocketTimeout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { }
          }
          
          if (clientSocket != null) {
            logDebug("Serve: Accepted new client connection: " + clientSocket)
            try {
              threadPool.execute(new ServeSingleRequest(clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close()
            }
          }
        }
      } finally {
        if (serverSocket != null) {
          logInfo("ServeMultipleRequests now stopping...")
          serverSocket.close()
        }
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }

    class ServeSingleRequest(val clientSocket: Socket)
    extends Thread with Logging {
      private val oos = new ObjectOutputStream(clientSocket.getOutputStream)
      oos.flush()
      private val ois = new ObjectInputStream(clientSocket.getInputStream)

      private var sendFrom = 0
      private var sendUntil = totalBlocks

      override def run() {
        try {
          logInfo("new ServeSingleRequest is running")

          // Receive range to send
          var rangeToSend = ois.readObject.asInstanceOf[(Int, Int)]
          sendFrom = rangeToSend._1
          sendUntil = rangeToSend._2

          // If not a valid range, stop broadcast
          if (sendFrom == SourceInfo.StopBroadcast && sendUntil == SourceInfo.StopBroadcast) {
            stopBroadcast = true
          } else {
            sendObject
          }
        } catch {
          case e: Exception => logError("ServeSingleRequest had a " + e)
        } finally {
          logInfo("ServeSingleRequest is closing streams and sockets")
          ois.close()
          oos.close()
          clientSocket.close()
        }
      }

      private def sendObject() {
        // Wait till receiving the SourceInfo from Driver
        while (totalBlocks == -1) {
          totalBlocksLock.synchronized { totalBlocksLock.wait() }
        }

        for (i <- sendFrom until sendUntil) {
          while (i == hasBlocks) {
            hasBlocksLock.synchronized { hasBlocksLock.wait() }
          }
          try {
            oos.writeObject(arrayOfBlocks(i))
            oos.flush()
          } catch {
            case e: Exception => logError("sendObject had a " + e)
          }
          logDebug("Sent block: " + i + " to " + clientSocket)
        }
      }
    }
  }
}

private[spark] class TreeBroadcastFactory
extends BroadcastFactory {
  def initialize(isDriver: Boolean) { MultiTracker.initialize(isDriver) }

  def newBroadcast[T](value_ : T, isLocal: Boolean, id: Long) =
    new TreeBroadcast[T](value_, isLocal, id)

  def stop() { MultiTracker.stop() }
}
