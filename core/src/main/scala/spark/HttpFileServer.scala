package spark

import java.io.{File, PrintWriter}
import java.net.URL
import scala.collection.mutable.HashMap
import org.apache.hadoop.fs.FileUtil

class HttpFileServer extends Logging {
  
  var baseDir : File = null
  var fileDir : File = null
  var jarDir : File = null
  var httpServer : HttpServer = null
  var serverUri : String = null
  
  def initialize() {
    baseDir = Utils.createTempDir()
    fileDir = new File(baseDir, "files")
    jarDir = new File(baseDir, "jars")
    fileDir.mkdir()
    jarDir.mkdir()
    logInfo("HTTP File server directory is " + baseDir)
    httpServer = new HttpServer(fileDir)
    httpServer.start()
    serverUri = httpServer.uri
  }
  
  def stop() {
    httpServer.stop()
  }
  
  def addFile(file: File) : String = {
    return addFileToDir(file, fileDir)
  }
  
  def addJar(file: File) : String = {
    return addFileToDir(file, jarDir)
  }
  
  def addFileToDir(file: File, dir: File) : String = {
    Utils.copyFile(file, new File(dir, file.getName))
    return dir + "/" + file.getName
  }
  
}