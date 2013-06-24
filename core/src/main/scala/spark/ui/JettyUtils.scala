package spark.ui

import annotation.tailrec

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import net.liftweb.json.{JValue, pretty, render}

import org.eclipse.jetty.server.{Server, Request, Handler}
import org.eclipse.jetty.server.handler.{ResourceHandler, HandlerList, ContextHandler, AbstractHandler}

import scala.util.{Try, Success, Failure}
import scala.xml.Node

import spark.{SparkContext, Logging}
import org.eclipse.jetty.util.log.Log

/** Utilities for launching a web server using Jetty's HTTP Server class */
private[spark] object JettyUtils extends Logging {
  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.
  type Responder[T] = HttpServletRequest => T

  // Conversions from various types of Responder's to jetty Handlers
  implicit def jsonResponderToHandler(responder: Responder[JValue]): Handler =
    createHandler(responder, "text/json", (in: JValue) => pretty(render(in)))

  implicit def htmlResponderToHandler(responder: Responder[Seq[Node]]): Handler =
    createHandler(responder, "text/html")

  implicit def textResponderToHandler(responder: Responder[String]): Handler =
    createHandler(responder, "text/plain")

  private def createHandler[T <% AnyRef](responder: Responder[T], contentType: String,
                                 extractFn: T => String = (in: Any) => in.toString): Handler = {
    new AbstractHandler {
      def handle(target: String,
                 baseRequest: Request,
                 request: HttpServletRequest,
                 response: HttpServletResponse) {
        response.setContentType("%s;charset=utf-8".format(contentType))
        response.setStatus(HttpServletResponse.SC_OK)
        baseRequest.setHandled(true)
        val result = responder(request)
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
        response.getWriter().println(extractFn(result))
      }
    }
  }

  /** Creates a handler that always redirects the user to a given path */
  def createRedirectHandler(newPath: String): Handler = {
    new AbstractHandler {
      def handle(target: String,
                 baseRequest: Request,
                 request: HttpServletRequest,
                 response: HttpServletResponse) {
        response.setStatus(302)
        response.setHeader("Location", baseRequest.getRootURL + newPath)
        baseRequest.setHandled(true)
      }
    }
  }

  /** Creates a handler for serving files from a static directory. */
  def createStaticHandler(resourceBase: String): ResourceHandler = {
    val staticHandler = new ResourceHandler
    Option(getClass.getClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        staticHandler.setResourceBase(res.toString)
      case None =>
        logError("Could not find resource path for Web UI: " + resourceBase)
    }
    staticHandler
  }

  /**
   * Attempts to start a Jetty server at the supplied ip:port which uses the supplied handlers.
   *
   * If the desired port number is contented, continues incrementing ports until a free port is
   * found. Returns the chosen port and the jetty Server object.
   */
  def startJettyServer(ip: String, port: Int, handlers: Seq[(String, Handler)]): (Server, Int) = {
    val handlersToRegister = handlers.map { case(path, handler) =>
      val contextHandler = new ContextHandler(path)
      contextHandler.setHandler(handler)
      contextHandler.asInstanceOf[org.eclipse.jetty.server.Handler]
    }

    val handlerList = new HandlerList
    handlerList.setHandlers(handlersToRegister.toArray)

    @tailrec
    def connect(currentPort: Int): (Server, Int) = {
      val server = new Server(currentPort)
      server.setHandler(handlerList)
      Try { server.start() } match {
        case s: Success[_] => (server, currentPort)
        case f: Failure[_] =>
          server.stop()
          logInfo("Failed to create UI at port, %s. Trying again.".format(currentPort))
          logInfo("Error was: " + f.toString)
          connect((currentPort + 1) % 65536)
      }
    }
    connect(port)
  }
}

/** Utility functions for generating XML pages with spark content. */
object UIUtils {

  /** Returns a page containing the supplied content and the spark web ui headers */
  def headerSparkPage(content: => Seq[Node], sc: SparkContext, title: String): Seq[Node] = {
    val newContent =
      <div class="row" style="padding-top: 5px;">
        <div class="span2">
          <div style="padding-left: 10px">
            <ul class="unstyled">
              <li><a href="/storage">Storage</a></li>
              <li><a href="/jobs">Jobs</a></li>
            </ul>
          </div>
        </div>
        <div class="span10">
          <ul class="unstyled">
            <li><strong>Master:</strong> {sc.master}</li>
            <li><strong>Application:</strong> {sc.appName}</li>
            <li><strong>Executors:</strong> {sc.getExecutorStorageStatus.size} </li>
          </ul>
        </div>
      </div>
      <hr/>;
    sparkPage(newContent ++ content, title)
  }

  /** Returns a page containing the supplied content and the spark css, js, and logo. */
  def sparkPage(content: => Seq[Node], title: String): Seq[Node] = {
    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href="/static/bootstrap.min.css" type="text/css" />
        <link rel="stylesheet" href="/static/bootstrap-responsive.min.css" type="text/css" />
        <script src="/static/sorttable.js"></script>
        <title>{title}</title>
        <style type="text/css">
          table.sortable thead {{ cursor: pointer; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="row">
            <div class="span2">
              <img src="/static/spark_logo.png" />
            </div>
            <div class="span10">
              <h1 style="vertical-align: bottom; margin-top: 40px; display: inline-block;">
                {title}
              </h1>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

  /** Returns an HTML table constructed by generating a row for each object in a sequence. */
  def listingTable[T](headers: Seq[String], makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
    <thead>{headers.map(h => <th>{h}</th>)}</thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }
}
