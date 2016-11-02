package org.apache.spark.streaming.status.api.v1

import org.apache.spark.status.api.v1.SimpleDateParam
import org.apache.spark.status.api.v1.UIRoot

import javax.ws.rs.GET
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StreamingInfoResource(uiRoot: UIRoot){
  @GET
  def streamingInfo()
  :Iterator[StreamingInfo]={
    Iterator(new StreamingInfo("testname",10L))

  }
}