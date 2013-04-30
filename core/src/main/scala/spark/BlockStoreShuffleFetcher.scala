package spark

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import spark.executor.{ShuffleReadMetrics, TaskMetrics}
import spark.serializer.Serializer
import spark.storage.BlockManagerId
import spark.util.CompletionIterator


private[spark] class BlockStoreShuffleFetcher extends ShuffleFetcher with Logging {

  override def fetch[K, V](
    shuffleId: Int, reduceId: Int, metrics: TaskMetrics, serializer: Serializer) = {

    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => ("shuffle_%d_%d_%d".format(shuffleId, s._1, reduceId), s._2)))
    }

    def unpackBlock(blockPair: (String, Option[Iterator[Any]])) : Iterator[(K, V)] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[(K, V)]]
        }
        case None => {
          val regex = "shuffle_([0-9]*)_([0-9]*)_([0-9]*)".r
          blockId match {
            case regex(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }

    val blockFetcherItr = blockManager.getMultiple(blocksByAddress, serializer)
    val itr = blockFetcherItr.flatMap(unpackBlock)

    CompletionIterator[(K,V), Iterator[(K,V)]](itr, {
      val shuffleMetrics = new ShuffleReadMetrics
      shuffleMetrics.remoteFetchTime = blockFetcherItr.remoteFetchTime
      shuffleMetrics.fetchWaitTime = blockFetcherItr.fetchWaitTime
      shuffleMetrics.remoteBytesRead = blockFetcherItr.remoteBytesRead
      shuffleMetrics.totalBlocksFetched = blockFetcherItr.totalBlocks
      shuffleMetrics.localBlocksFetched = blockFetcherItr.numLocalBlocks
      shuffleMetrics.remoteBlocksFetched = blockFetcherItr.numRemoteBlocks
      metrics.shuffleReadMetrics = Some(shuffleMetrics)
    })
  }
}
