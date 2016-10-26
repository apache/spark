package org.apache.spark.streaming.status.api.v1

import javax.ws.rs.Path
import javax.servlet.ServletContext
import org.apache.spark.status.api.v1.UIRoot
import org.eclipse.jetty.server.handler.ContextHandler
import javax.ws.rs.core.Context

@Path("/streaming/v1")
private[v1] class StreamingApiRootResource extends UIRootFromServletContext{

  @Path("streaminginfo")
  def getStreamingInfo(): StreamingInfoResource = {
    new StreamingInfoResource(uiRoot)
  }
}

private[v1] object UIRootFromServletContext {

  private val attribute = getClass.getCanonicalName

  def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit = {
    contextHandler.setAttribute(attribute, uiRoot)
  }

  def getUiRoot(context: ServletContext): UIRoot = {
    context.getAttribute(attribute).asInstanceOf[UIRoot]
  }
}

private[v1] trait UIRootFromServletContext {
  @Context
  var servletContext: ServletContext = _

  def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)
}