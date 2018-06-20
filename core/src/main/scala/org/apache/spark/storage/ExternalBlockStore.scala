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

package org.apache.spark.storage

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

import scala.util.control.NonFatal


/**
  * Stores BlockManager blocks on ExternalBlockStore.
  * We capture any potential exception from underlying implementation
  * and return with the expected failure value
  */
private[spark] class ExternalBlockStore(blockManager: BlockManager) extends Logging {

  lazy val externalBlockManager: Option[ExternalBlockManager] = createBlkManager()

  logInfo("ExternalBlockStore started")

  // Create concrete block manager and fall back to Tachyon by default for backward compatibility.
  private def createBlkManager(): Option[ExternalBlockManager] = {
    val clsName = blockManager.conf.getOption(ExternalBlockStore.BLOCK_MANAGER_NAME)
      .getOrElse(ExternalBlockStore.DEFAULT_BLOCK_MANAGER_NAME)

    try {
      val instance = Utils.classForName(clsName)
        .newInstance()
        .asInstanceOf[ExternalBlockManager]
      instance.init(blockManager)
      ShutdownHookManager.addShutdownHook { () =>
        logDebug("Shutdown hook called")
        externalBlockManager.map(_.shutdown())
      }
      Some(instance)
    } catch {
      case NonFatal(t) =>
        logError("Cannot initialize external block store", t)
        None
    }
  }
}

private[spark] object ExternalBlockStore extends Logging {
  val MAX_DIR_CREATION_ATTEMPTS = 10
  val SUB_DIRS_PER_DIR = "64"
  val BASE_DIR = "spark.externalBlockStore.baseDir"
  val FOLD_NAME = "spark.externalBlockStore.folderName"
  val MASTER_URL = "spark.externalBlockStore.url"
  val BLOCK_MANAGER_NAME = "spark.externalBlockStore.blockManager"
  val DEFAULT_BLOCK_MANAGER_NAME = "org.apache.spark.storage.AlluxioBlockManager"
}