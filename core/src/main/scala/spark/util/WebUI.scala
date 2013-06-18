package spark.util

import xml.Elem
import xml.Node
import util.parsing.json.{JSONFormat, JSONObject}
import org.eclipse.jetty.server.{Server, Request, Handler}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.eclipse.jetty.util.component.LifeCycle.Listener
import org.eclipse.jetty.server.handler.{ResourceHandler, HandlerList, ContextHandler, AbstractHandler}
import util.Try
import util.Success
import util.Failure
import spark.Logging
import annotation.tailrec

object WebUI extends Logging {
  type Responder[T] = HttpServletRequest => T

  implicit def jsonResponderToHandler(responder: Responder[JSONObject]): Handler = {
    createHandler(responder, "text/json")
  }

  implicit def htmlResponderToHandler(responder: Responder[Seq[Node]]): Handler = {
    createHandler(responder, "text/html")
  }

  def createHandler[T <% AnyRef](responder: Responder[T], contentType: String): Handler = {
    new AbstractHandler {
      def handle(target: String,
                 baseRequest: Request,
                 request: HttpServletRequest,
                 response: HttpServletResponse) {
        response.setContentType("%s;charset=utf-8".format(contentType))
        response.setStatus(HttpServletResponse.SC_OK)
        baseRequest.setHandled(true)
        response.getWriter().println(
          responder(request).toString
        )
      }
    }
  }

  def createStaticHandler(resourceBase: String): ResourceHandler = {
    val staticHandler = new ResourceHandler
    val resource = getClass.getClassLoader.getResource(resourceBase)
    staticHandler.setResourceBase(resource.toString)
    staticHandler
  }

  def startJettyServer(ip: String, port: Int, handlers: Array[(String, Handler)]): (Server, Int) = {
    val handlersToRegister = handlers.map { case(path, handler) =>
      if (path == "*") {
        handler
      } else {
        val contextHandler = new ContextHandler(path)
        contextHandler.setHandler(handler)
        contextHandler.asInstanceOf[org.eclipse.jetty.server.Handler]
      }
    }

    val handlerList = new HandlerList
    handlerList.setHandlers(handlersToRegister)

    @tailrec
    def connect(currentPort: Int): (Server, Int) = {
      val server = new Server(port)
      server.setHandler(handlerList)
      Try { server.start() } match {
        case s: Success[_] => (server, currentPort)
        case f: Failure[_] =>
          logInfo("Failed to create UI at port, %s. Trying again.".format(currentPort))
          connect((currentPort + 1) % 65536)
      }
    }

    connect(port)
  }

  def makePage(content: => Seq[Node], title: String): Seq[Node] = {
    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href="/static/bootstrap.min.css" type="text/css" />
        <link rel="stylesheet" href="/static/bootstrap-responsive.min.css" type="text/css" />
        <script src="/static/sorttable.js"></script>
        <title>{title}</title>
      </head>
      <body>
        <div class="container">
          <div class="row">
            <div class="span12">
              <img src="/static/spark_logo.png" />
              <h1 style="vertical-align: bottom; margin-bottom: 10px;
                         margin-left: 30px; display: inline-block;">
              {title}
              </h1>
            </div>
          </div>
        {content}
      </div>
    </body>
  </html>
  }
}
