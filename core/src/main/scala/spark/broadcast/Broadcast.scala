package spark.broadcast

import java.io._
import java.net._
import java.util.{BitSet, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.Map

import spark._

trait Broadcast[T] extends Serializable {
  val uuid = UUID.randomUUID

  def value: T

  // We cannot have an abstract readObject here due to some weird issues with
  // readObject having to be 'private' in sub-classes.

  override def toString = "spark.Broadcast(" + uuid + ")"
}

object Broadcast extends Logging with Serializable {

  private var initialized = false
  private var isMaster_ = false
  private var broadcastFactory: BroadcastFactory = null

  // Cache of broadcasted objects
  val values = SparkEnv.get.cache.newKeySpace()

  // Called by SparkContext or Executor before using Broadcast
  def initialize(isMaster__ : Boolean) {
    synchronized {
      if (!initialized) {
        val broadcastFactoryClass = System.getProperty(
          "spark.broadcast.factory", "spark.broadcast.HttpBroadcastFactory")

        broadcastFactory =
          Class.forName(broadcastFactoryClass).newInstance.asInstanceOf[BroadcastFactory]

        // Setup isMaster before using it
        isMaster_ = isMaster__

        // Set masterHostAddress to the master's IP address for the slaves to read
        if (isMaster) {
          System.setProperty("spark.broadcast.masterHostAddress", Utils.localIpAddress)
        }

        // Initialize appropriate BroadcastFactory and BroadcastObject
        broadcastFactory.initialize(isMaster)

        initialized = true
      }
    }
  }

  def stop() {
    // TODO:
  }

  def getBroadcastFactory: BroadcastFactory = {
    if (broadcastFactory == null) {
      throw new SparkException ("Broadcast.getBroadcastFactory called before initialize")
    }
    broadcastFactory
  }

  private var MasterHostAddress_ = System.getProperty(
    "spark.broadcast.masterHostAddress", "")

  def isMaster = isMaster_
  
  def MasterHostAddress = MasterHostAddress_
}

case class BroadcastBlock(blockID: Int, byteArray: Array[Byte]) 
extends Serializable

case class VariableInfo(@transient arrayOfBlocks : Array[BroadcastBlock],
                        totalBlocks: Int, 
                        totalBytes: Int) 
extends Serializable {
 @transient var hasBlocks = 0 
}
