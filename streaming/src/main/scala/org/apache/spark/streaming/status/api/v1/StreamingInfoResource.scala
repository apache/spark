package org.apache.spark.streaming.status.api.v1

import org.apache.spark.status.api.v1.SimpleDateParam
import org.apache.spark.status.api.v1.UIRoot

import javax.ws.rs.GET
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.ui.StreamingJobProgressListener

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StreamingInfoResource(uiRoot: UIRoot, listener: StreamingJobProgressListener){

  @GET
  def streamingInfo()
  :Iterator[StreamingInfo]={
    var v = listener.numTotalCompletedBatches
    Iterator(new StreamingInfo("testname",v))

  }
}