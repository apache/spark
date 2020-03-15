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

package org.apache.spark.sql.jdbc

import java.io.{File, FileInputStream, FileOutputStream}
import javax.security.auth.login.Configuration

import scala.io.Source

import org.apache.hadoop.minikdc.MiniKdc

import org.apache.spark.util.{SecurityUtils, Utils}

abstract class DockerKrbJDBCIntegrationSuite extends DockerJDBCIntegrationSuite {
  private var kdc: MiniKdc = _
  protected var workDir: File = _
  protected val userName: String
  protected var principal: String = _
  protected val keytabFileName: String
  protected var keytabFullPath: String = _
  protected def setAuthentication(keytabFile: String, principal: String): Unit

  override def beforeAll(): Unit = {
    SecurityUtils.setGlobalKrbDebug(true)

    val kdcDir = Utils.createTempDir()
    val kdcConf = MiniKdc.createConf()
    kdcConf.setProperty(MiniKdc.DEBUG, "true")
    kdc = new MiniKdc(kdcConf, kdcDir)
    kdc.start()

    principal = s"$userName@${kdc.getRealm}"

    workDir = Utils.createTempDir()
    val keytabFile = new File(workDir, keytabFileName)
    keytabFullPath = keytabFile.getAbsolutePath
    kdc.createPrincipal(keytabFile, userName)
    logInfo(s"Created keytab file: $keytabFullPath")

    setAuthentication(keytabFullPath, principal)

    // This must be executed intentionally later
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (kdc != null) {
        kdc.stop()
      }
      Configuration.setConfiguration(null)
      SecurityUtils.setGlobalKrbDebug(false)
    } finally {
      super.afterAll()
    }
  }

  protected def copyExecutableResource(
      fileName: String, dir: File, processLine: String => String) = {
    val newEntry = new File(dir.getAbsolutePath, fileName)
    newEntry.createNewFile()
    Utils.tryWithResource(
      new FileInputStream(getClass.getClassLoader.getResource(fileName).getFile)
    ) { inputStream =>
      val outputStream = new FileOutputStream(newEntry)
      try {
        for (line <- Source.fromInputStream(inputStream).getLines()) {
          val processedLine = processLine(line) + System.lineSeparator()
          outputStream.write(processedLine.getBytes)
        }
      } finally {
        outputStream.close()
      }
    }
    newEntry.setExecutable(true, false)
    logInfo(s"Created executable resource file: ${newEntry.getAbsolutePath}")
    newEntry
  }
}
