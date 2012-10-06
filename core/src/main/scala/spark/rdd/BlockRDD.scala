package spark.rdd

import scala.collection.mutable.HashMap

import spark.Dependency
import spark.RDD
import spark.SparkContext
import spark.SparkEnv
import spark.Split

private[spark] class BlockRDDSplit(val blockId: String, idx: Int) extends Split {
  val index = idx
}

private[spark]
class BlockRDD[T: ClassManifest](sc: SparkContext, @transient blockIds: Array[String])
  extends RDD[T](sc) {

  @transient
  val splits_ = (0 until blockIds.size).map(i => {
    new BlockRDDSplit(blockIds(i), i).asInstanceOf[Split]
  }).toArray 
  
  @transient 
  lazy val locations_  = {
    val blockManager = SparkEnv.get.blockManager 
    /*val locations = blockIds.map(id => blockManager.getLocations(id))*/
    val locations = blockManager.getLocations(blockIds) 
    HashMap(blockIds.zip(locations):_*)
  }

  override def splits = splits_

  override def compute(split: Split): Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager 
    val blockId = split.asInstanceOf[BlockRDDSplit].blockId
    blockManager.get(blockId) match {
      case Some(block) => block.asInstanceOf[Iterator[T]]
      case None => 
        throw new Exception("Could not compute split, block " + blockId + " not found")
    }
  }

  override def preferredLocations(split: Split) = 
    locations_(split.asInstanceOf[BlockRDDSplit].blockId)

  override val dependencies: List[Dependency[_]] = Nil
}

