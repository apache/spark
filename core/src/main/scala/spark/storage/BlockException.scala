package spark.storage

private[spark]
case class BlockException(blockId: String, message: String) extends Exception(message)

