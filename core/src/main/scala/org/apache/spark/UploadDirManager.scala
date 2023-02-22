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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Kubernetes upload directory manager. It's responsible for cleanup of the upload directory
 * when spark job finishes.
 */
private[spark] class UploadDirManager(conf: SparkConf, appId: String) extends Logging {
  require(conf.contains("spark.kubernetes.file.upload.path"))

  private val uploadPath = new Path(conf.get("spark.kubernetes.file.upload.path"))
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fileSystem = FileSystem.get(uploadPath.toUri, hadoopConf)
  private val uploadDirName = s"${uploadPath}/spark-upload-${appId}"
  private val uploadDirPath = new Path(uploadDirName)

  private val shutdownHook = addShutdownHook()

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook")
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY - 1) { () =>
      logInfo("Shutdown hook called")
      Utils.tryLogNonFatalError {
        UploadDirManager.this.cleanUp()
      }
    }
  }

  def cleanUp(): Unit = {
    if (shutdownHook != null) {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    }
    if (fileSystem.exists(uploadDirPath)) {
      if (fileSystem.delete(uploadDirPath, true)) {
        logInfo(s"Upload dir deleted successfully.: $uploadDirPath")
      } else {
        logWarning(s"Failed to delete upload dir: $uploadDirPath")
      }
    } else {
      logInfo("upload dir doesn't exist")
    }
  }
}

private[spark] object UploadDirManager extends Logging {

  def init(conf: SparkConf, appId: String): Option[UploadDirManager] = {
    if (conf.contains("spark.kubernetes.file.upload.path")) {
      logInfo("creating instance of UploadDirManager")
      Some(new UploadDirManager(conf, appId))
    } else {
      logInfo("UploadDirManager instance not created")
      None
    }
  }
}
