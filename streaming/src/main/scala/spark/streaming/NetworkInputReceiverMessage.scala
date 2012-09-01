package spark.streaming

sealed trait NetworkInputReceiverMessage

case class GetBlockIds(time: Long) extends NetworkInputReceiverMessage
case class GotBlockIds(streamId: Int, blocksIds: Array[String]) extends NetworkInputReceiverMessage
case object StopReceiver extends NetworkInputReceiverMessage
