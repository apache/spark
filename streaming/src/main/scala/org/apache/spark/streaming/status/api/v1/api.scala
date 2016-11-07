package org.apache.spark.streaming.status.api.v1

class StreamingInfo private[streaming](
    val name:String,
    val completedBatchCount:Long)
  