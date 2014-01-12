/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.{File}
import com.google.common.io.Files
import org.apache.spark.util.Utils

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
    serverUri + "/files/" + file.getName
  }
  
  def addJar(file: File) : String = {
    addFileToDir(file, jarDir)
    serverUri + "/jars/" + file.getName
  }
  
  def addFileToDir(file: File, dir: File) : String = {
    Files.copy(file, new File(dir, file.getName))
    dir + "/" + file.getName
  }
  
}
