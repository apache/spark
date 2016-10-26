package org.apache.spark.streaming.status.api.v1

import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import org.apache.spark.status.api.v1.UIRoot
import javax.swing.JList
import javax.ws.rs.QueryParam
import org.apache.spark.status.api.v1.SimpleDateParam
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StreamingInfoResource(uiRoot: UIRoot){
  @GET
  def streamingInfo()
  :StreamingInfo={
    new StreamingInfo("testname",10L)
  }
}