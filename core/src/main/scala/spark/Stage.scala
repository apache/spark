package spark

class Stage(val id: Int, val rdd: RDD[_], val shuffleDep: Option[ShuffleDependency[_,_,_]], val parents: List[Stage]) {
  val isShuffleMap = shuffleDep != None
  val numPartitions = rdd.splits.size
  val outputLocs = Array.fill[List[String]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  def isAvailable: Boolean = {
    if (parents.size == 0 && !isShuffleMap)
      true
    else
      numAvailableOutputs == numPartitions
  }

  def addOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = host :: prevList
    if (prevList == Nil)
      numAvailableOutputs += 1
  }

  def removeOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    val newList = prevList - host
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil)
      numAvailableOutputs -= 1
  }

  override def toString = "Stage " + id

  override def hashCode(): Int = id
}
