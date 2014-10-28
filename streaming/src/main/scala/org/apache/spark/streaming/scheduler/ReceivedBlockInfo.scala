package org.apache.spark.streaming.scheduler

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.util.WriteAheadLogFileSegment
import org.apache.spark.annotation.DeveloperApi

/** Information about blocks received by the receiver */
private[streaming] case class ReceivedBlockInfo(
    streamId: Int,
    blockId: StreamBlockId,
    numRecords: Long,
    metadata: Any,
    persistenceInfoOption: Option[Any]
  )

