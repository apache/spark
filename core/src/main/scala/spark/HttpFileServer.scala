package spark

import java.io.{File}
import com.google.common.io.Files

private[spark] class HttpFileServer extends Logging {
  
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
    httpServer = new HttpServer(baseDir)
    httpServer.start()
    serverUri = httpServer.uri
  }
  
  def stop() {
    httpServer.stop()
  }
  
  def addFile(file: File) : String = {
    addFileToDir(file, fileDir)
    return serverUri + "/files/" + file.getName
  }
  
  def addJar(file: File) : String = {
    addFileToDir(file, jarDir)
    return serverUri + "/jars/" + file.getName
  }
  
  def addFileToDir(file: File, dir: File) : String = {
    Files.copy(file, new File(dir, file.getName))
    return dir + "/" + file.getName
  }
  
}