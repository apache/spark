package spark.storage

import spark.{Utils, SparkContext}
import BlockManagerMasterActor.BlockStatus

private[spark]
case class StorageStatus(blockManagerId: BlockManagerId, maxMem: Long,
  blocks: Map[String, BlockStatus]) {

  def memUsed(blockPrefix: String = "") = {
    blocks.filterKeys(_.startsWith(blockPrefix)).values.map(_.memSize).
      reduceOption(_+_).getOrElse(0l)
  }

  def diskUsed(blockPrefix: String = "") = {
    blocks.filterKeys(_.startsWith(blockPrefix)).values.map(_.diskSize).
      reduceOption(_+_).getOrElse(0l)
  }

  def memRemaining : Long = maxMem - memUsed()

}

case class RDDInfo(id: Int, name: String, storageLevel: StorageLevel,
  numCachedPartitions: Int, numPartitions: Int, memSize: Long, diskSize: Long)
  extends Ordered[RDDInfo] {
  override def toString = {
    import Utils.memoryBytesToString
    "RDD \"%s\" (%d) Storage: %s; CachedPartitions: %d; TotalPartitions: %d; MemorySize: %s; DiskSize: %s".format(name, id,
      storageLevel.toString, numCachedPartitions, numPartitions, memoryBytesToString(memSize), memoryBytesToString(diskSize))
  }

  override def compare(that: RDDInfo) = {
    this.id - that.id
  }
}

/* Helper methods for storage-related objects */
private[spark]
object StorageUtils {

  /* Given the current storage status of the BlockManager, returns information for each RDD */
  def rddInfoFromStorageStatus(storageStatusList: Array[StorageStatus],
    sc: SparkContext) : Array[RDDInfo] = {
    rddInfoFromBlockStatusList(storageStatusList.flatMap(_.blocks).toMap, sc)
  }

  /* Given a list of BlockStatus objets, returns information for each RDD */
  def rddInfoFromBlockStatusList(infos: Map[String, BlockStatus],
    sc: SparkContext) : Array[RDDInfo] = {

    // Group by rddId, ignore the partition name
    val groupedRddBlocks = infos.filterKeys(_.startsWith("rdd_")).groupBy { case(k, v) =>
      k.substring(0,k.lastIndexOf('_'))
    }.mapValues(_.values.toArray)

    // For each RDD, generate an RDDInfo object
    val rddInfos = groupedRddBlocks.map { case(rddKey, rddBlocks) =>

      // Add up memory and disk sizes
      val memSize = rddBlocks.map(_.memSize).reduce(_ + _)
      val diskSize = rddBlocks.map(_.diskSize).reduce(_ + _)

      // Find the id of the RDD, e.g. rdd_1 => 1
      val rddId = rddKey.split("_").last.toInt
      // Get the friendly name for the rdd, if available.
      val rdd = sc.persistentRdds(rddId)
      val rddName = Option(rdd.name).getOrElse(rddKey)
      val rddStorageLevel = rdd.getStorageLevel

      RDDInfo(rddId, rddName, rddStorageLevel, rddBlocks.length, rdd.partitions.size, memSize, diskSize)
    }.toArray

    scala.util.Sorting.quickSort(rddInfos)

    rddInfos
  }

  /* Removes all BlockStatus object that are not part of a block prefix */
  def filterStorageStatusByPrefix(storageStatusList: Array[StorageStatus],
    prefix: String) : Array[StorageStatus] = {

    storageStatusList.map { status =>
      val newBlocks = status.blocks.filterKeys(_.startsWith(prefix))
      //val newRemainingMem = status.maxMem - newBlocks.values.map(_.memSize).reduce(_ + _)
      StorageStatus(status.blockManagerId, status.maxMem, newBlocks)
    }

  }

}
