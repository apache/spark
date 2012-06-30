package spark

import java.io.EOFException
import java.net.URL

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import spark.storage.BlockException
import spark.storage.BlockManagerId

import it.unimi.dsi.fastutil.io.FastBufferedInputStream


class BlockStoreShuffleFetcher extends ShuffleFetcher with Logging {
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit) {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val ser = SparkEnv.get.serializer.newInstance()
    val blockManager = SparkEnv.get.blockManager
    
    val startTime = System.currentTimeMillis
    val addresses = SparkEnv.get.mapOutputTracker.getServerAddresses(shuffleId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))
    
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[Int]]
    for ((address, index) <- addresses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += index
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[String])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(i => "shuffleid_%d_%d_%d".format(shuffleId, i, reduceId)))
    }

    try {
      val blockOptions = blockManager.get(blocksByAddress)
      logDebug("Fetching map output blocks for shuffle %d, reduce %d took %d ms".format(
        shuffleId, reduceId, System.currentTimeMillis - startTime))
      blockOptions.foreach(x => {
        val (blockId, blockOption) = x 
        blockOption match {
          case Some(block) => {
            val values = block
            for(value <- values) {
              val v = value.asInstanceOf[(K, V)]
              func(v._1, v._2)
            }
          }
          case None => {
            throw new BlockException(blockId, "Did not get block " + blockId)         
          }
        }
      })
    } catch {
      case be: BlockException => {
        val regex = "shuffledid_([0-9]*)_([0-9]*)_([0-9]]*)".r
        be.blockId match {
          case regex(sId, mId, rId) => { 
            val address = addresses(mId.toInt)
            throw new FetchFailedException(address, sId.toInt, mId.toInt, rId.toInt, be)
          }
          case _ => {
            throw be
          }
        }
      }
    }
  }
}
