package spark

import java.io.{File, PrintWriter}
import java.net.URL
import scala.collection.mutable.HashMap
import org.apache.hadoop.fs.FileUtil

class HttpFileServer extends Logging {
  
  var fileDir : File = null
  var httpServer : HttpServer = null
  var serverUri : String = null
  
  def initialize() {
    fileDir = Utils.createTempDir()
    logInfo("HTTP File server directory is " + fileDir)
    httpServer = new HttpServer(fileDir)
    httpServer.start()
    serverUri = httpServer.uri
  }
  
  def addFile(file: File) : String = {
    Utils.copyFile(file, new File(fileDir, file.getName))
    return serverUri + "/" + file.getName
  }
  
  def stop() {
    httpServer.stop()
  }
  
}