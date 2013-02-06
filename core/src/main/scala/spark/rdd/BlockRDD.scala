package spark.rdd

import scala.collection.mutable.HashMap
import spark.{RDD, SparkContext, SparkEnv, Split, TaskContext}

private[spark] class BlockRDDSplit(val blockId: String, idx: Int) extends Split {
  val index = idx
}

private[spark]
class BlockRDD[T: ClassManifest](sc: SparkContext, @transient blockIds: Array[String])
  extends RDD[T](sc, Nil) {

  @transient lazy val locations_  = {
    val blockManager = SparkEnv.get.blockManager
    /*val locations = blockIds.map(id => blockManager.getLocations(id))*/
    val locations = blockManager.getLocations(blockIds)
    HashMap(blockIds.zip(locations):_*)
  }

  override def getSplits = (0 until blockIds.size).map(i => {
    new BlockRDDSplit(blockIds(i), i).asInstanceOf[Split]
  }).toArray


  override def compute(split: Split, context: TaskContext): Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDSplit].blockId
    blockManager.get(blockId) match {
      case Some(block) => block.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception("Could not compute split, block " + blockId + " not found")
    }
  }

  override def getPreferredLocations(split: Split) =
    locations_(split.asInstanceOf[BlockRDDSplit].blockId)

}

