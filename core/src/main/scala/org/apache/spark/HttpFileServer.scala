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

import java.io.File

import com.google.common.io.Files

import org.apache.spark.util.Utils

private[spark] class HttpFileServer(
    conf: SparkConf,
    securityManager: SecurityManager,
    requestedPort: Int = 0)
  extends Logging {

  var baseDir : File = null
  var fileDir : File = null
  var jarDir : File = null
  var httpServer : HttpServer = null
  var serverUri : String = null

  def initialize() {
    baseDir = Utils.createTempDir(Utils.getLocalDir(conf), "httpd")
    fileDir = new File(baseDir, "files")
    jarDir = new File(baseDir, "jars")
    fileDir.mkdir()
    jarDir.mkdir()
    logInfo("HTTP File server directory is " + baseDir)
    httpServer = new HttpServer(conf, baseDir, securityManager, requestedPort, "HTTP file server")
    httpServer.start()
    serverUri = httpServer.uri
    logDebug("HTTP file server started at: " + serverUri)
  }

  def stop() {
    httpServer.stop()

    // If we only stop sc, but the driver process still run as a services then we need to delete
    // the tmp dir, if not, it will create too many tmp dirs
    try {
      Utils.deleteRecursively(baseDir)
    } catch {
      case e: Exception =>
        logWarning(s"Exception while deleting Spark temp dir: ${baseDir.getAbsolutePath}", e)
    }
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
    // Check whether the file is a directory. If it is, throw a more meaningful exception.
    // If we don't catch this, Guava throws a very confusing error message:
    //   java.io.FileNotFoundException: [file] (No such file or directory)
    // even though the directory ([file]) exists.
    if (file.isDirectory) {
      throw new IllegalArgumentException(s"$file cannot be a directory.")
    }
    Files.copy(file, new File(dir, file.getName))
    dir + "/" + file.getName
  }

}
