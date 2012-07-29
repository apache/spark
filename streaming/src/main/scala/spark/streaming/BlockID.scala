package spark.streaming

case class BlockID(sRds: String, sInterval: Interval, sPartition: Int) {
  override def toString : String = (
    sRds + BlockID.sConnector + 
    sInterval.beginTime + BlockID.sConnector + 
    sInterval.endTime + BlockID.sConnector +
    sPartition
  )
}

object BlockID {
  val sConnector = '-'

  def parse(name : String) = BlockID(
      name.split(BlockID.sConnector)(0),
      new Interval(name.split(BlockID.sConnector)(1).toLong, 
          name.split(BlockID.sConnector)(2).toLong),
      name.split(BlockID.sConnector)(3).toInt)
}