package org.apache.spark.streaming.receiver

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.util.WriteAheadLogFileSegment

/** Information about blocks received by the receiver */
private[streaming] case class ReceivedBlockInfo(
    streamId: Int,
    blockId: StreamBlockId,
    numRecords: Long,
    metadata: Any,
    fileSegmentOption: Option[WriteAheadLogFileSegment]
  )

