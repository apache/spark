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
import org.eclipse.jetty.util.thread.QueuedThreadPool

/** Utilities for launching a web server using Jetty's HTTP Server class */
private[spark] object JettyUtils extends Logging {
  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.
  type Responder[T] = HttpServletRequest => T

  // Conversions from various types of Responder's to jetty Handlers
  implicit def jsonResponderToHandler(responder: Responder[JValue]): Handler =
    createHandler(responder, "text/json", (in: JValue) => pretty(render(in)))

  implicit def htmlResponderToHandler(responder: Responder[Seq[Node]]): Handler =
    createHandler(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

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

  /** Creates a handler for serving files from a static directory */
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
      val pool = new QueuedThreadPool
      pool.setDaemon(true)
      server.setThreadPool(pool)
      server.setHandler(handlerList)

      Try { server.start() } match {
        case s: Success[_] =>
          sys.addShutdownHook(server.stop()) // Be kind, un-bind
          (server, server.getConnectors.head.getLocalPort)
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

object Page extends Enumeration { val Storage, Jobs = Value }

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils {
  import Page._

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(content: => Seq[Node], sc: SparkContext, title: String, page: Page.Value)
  : Seq[Node] = {
    val storage = page match {
      case Storage => <li class="active"><a href="/storage">Storage</a></li>
      case _ => <li><a href="/storage">Storage</a></li>
    }
    val jobs = page match {
      case Jobs => <li class="active"><a href="/stages">Jobs</a></li>
      case _ => <li><a href="/stages">Jobs</a></li>
    }

    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href="/static/bootstrap.min.css" type="text/css" />
        <link rel="stylesheet" href="/static/webui.css" type="text/css" />
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
            <div class="span12">
              <div class="navbar">
                <div class="navbar-inner">
                  <div class="container">
                    <div class="brand"><img src="/static/spark-logo-77x50px-hd.png" /></div>
                    <ul class="nav">
                      {storage}
                      {jobs}
                      </ul>
                    <ul id="infolist">
                      <li>Master: <strong>{sc.master}</strong></li>
                      <li>Application: <strong>{sc.appName}</strong></li>
                      <li>Executors: <strong>{sc.getExecutorStorageStatus.size}</strong></li>
                    </ul>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="row" style="padding-top: 5px;">
            <div class="span12">
              <h1 style="vertical-align: bottom; display: inline-block;">
                {title}
              </h1>
            </div>
          </div>
          <hr/>
          {content}
        </div>
      </body>
    </html>
  }

  /** Returns a page with the spark css/js and a simple format. Used for scheduler UI. */
  def basicSparkPage(content: => Seq[Node], title: String): Seq[Node] = {
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
