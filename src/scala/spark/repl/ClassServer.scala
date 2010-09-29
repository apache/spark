package spark.repl

import java.io.File
import java.net.InetAddress

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler


/**
 * Exception type thrown by ClassServer when it is in the wrong state 
 * for an operation.
 */
class ServerStateException(message: String) extends Exception(message)


/**
 * An HTTP server used by the interpreter to allow worker nodes to access
 * class files created as the user types in lines of code. This is just a
 * wrapper around a Jetty embedded HTTP server.
 */
class ClassServer(classDir: File) {
  private var server: Server = null
  private var port: Int = -1

  def start() {
    if (server != null) {
      throw new ServerStateException("Server is already started")
    } else {
      server = new Server(0)
      val resHandler = new ResourceHandler
      resHandler.setResourceBase(classDir.getAbsolutePath)
      val handlerList = new HandlerList
      handlerList.setHandlers(Array(resHandler, new DefaultHandler))
      server.setHandler(handlerList)
      server.start()
      port = server.getConnectors()(0).getLocalPort()
    }
  }

  def stop() {
    if (server == null) {
      throw new ServerStateException("Server is already stopped")
    } else {
      server.stop()
      port = -1
      server = null
    }
  }

  /**
   * Get the URI of this HTTP server (http://host:port)
   */
  def uri: String = {
    if (server == null) {
      throw new ServerStateException("Server is not started")
    } else {
      return "http://" + getLocalIpAddress + ":" + port
    }
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4)
   */
  private def getLocalIpAddress: String = {
    // Get local IP as an array of four bytes
    val bytes = InetAddress.getLocalHost().getAddress()
    // Convert the bytes to ints (keeping in mind that they may be negative)
    // and join them into a string
    return bytes.map(b => (b.toInt + 256) % 256).mkString(".")
  }
}
