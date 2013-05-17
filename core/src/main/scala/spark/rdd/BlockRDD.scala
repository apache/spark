package spark.rdd

import spark.{RDD, SparkContext, SparkEnv, Partition, TaskContext}
import spark.storage.BlockManager

private[spark] class BlockRDDPartition(val blockId: String, idx: Int) extends Partition {
  val index = idx
}

private[spark]
class BlockRDD[T: ClassManifest](sc: SparkContext, @transient blockIds: Array[String])
  extends RDD[T](sc, Nil) {

  @transient lazy val locations_ = BlockManager.blockIdsToExecutorLocations(blockIds, SparkEnv.get)

  override def getPartitions: Array[Partition] = (0 until blockIds.size).map(i => {
    new BlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
  }).toArray


  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    blockManager.get(blockId) match {
      case Some(block) => block.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception("Could not compute split, block " + blockId + " not found")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    locations_(split.asInstanceOf[BlockRDDPartition].blockId)

}

