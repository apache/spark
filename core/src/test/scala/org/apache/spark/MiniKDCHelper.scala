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
import java.net.BindException

import scala.util.control.NonFatal

import org.apache.hadoop.minikdc.MiniKdc

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

trait MiniKDCHelper extends Logging {

  def startMiniKdc(): MiniKdc = {
    var kdc: MiniKdc = null
    val kdcConf = MiniKdc.createConf()
    kdcConf.setProperty(MiniKdc.DEBUG, "true")
    var bindException = false
    var kdcDir: File = null
    var numRetries = 1
    do {
      try {
        bindException = false
        kdcDir = Utils.createTempDir()
        kdc = new MiniKdc(kdcConf, kdcDir)
        kdc.start()
      } catch {
        case be: BindException if numRetries == 3 =>
          logError(s"Failed setting up MiniKDC. Tried $numRetries times.");
          throw be
        case be: BindException =>
          try {
            Utils.deleteRecursively(kdcDir)
          } catch {
            case NonFatal(_) =>
              logWarning("Failed to clean the working directory of unsuccessful MiniKdc")
          }
          numRetries += 1
          logWarning("Failed setting up MiniKdc, try again...", be)
          bindException = true
      }
    } while (bindException)
    kdc
  }

}
