package spark

import java.io.EOFException
import java.net.URL

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import spark.storage.BlockException
import spark.storage.BlockManagerId

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

private[spark] class BlockStoreShuffleFetcher extends ShuffleFetcher with Logging {
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit) {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
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
        (address, splits.map(i => "shuffle_%d_%d_%d".format(shuffleId, i, reduceId)))
    }

    for ((blockId, blockOption) <- blockManager.getMultiple(blocksByAddress)) {
      blockOption match {
        case Some(block) => {
          val values = block
          for(value <- values) {
            val v = value.asInstanceOf[(K, V)]
            func(v._1, v._2)
          }
        }
        case None => {
          val regex = "shuffle_([0-9]*)_([0-9]*)_([0-9]*)".r
          blockId match {
            case regex(shufId, mapId, reduceId) =>
              val addr = addresses(mapId.toInt)
              throw new FetchFailedException(addr, shufId.toInt, mapId.toInt, reduceId.toInt, null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }

    logDebug("Fetching and merging outputs of shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))
  }
}
