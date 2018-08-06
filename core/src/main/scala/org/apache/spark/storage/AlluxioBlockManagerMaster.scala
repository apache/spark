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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

private[spark] class AlluxioBlockManagerMaster extends Logging {

  private var chroot: Path = _
  private var fs : FileSystem = _

  override def toString: String = "ExternalBlockStore-Alluxio"

  def init(conf: SparkConf): Unit = {

    val masterUrl = conf.get(ExternalBlockStore.MASTER_URL, "alluxio://localhost:19998")
    val storeDir = conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_alluxio")
    val subDirsPerAlluxio = conf.getInt(
      "spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR.toInt)
    val folderName = conf.get(ExternalBlockStore.FOLD_NAME)

    val master = new Path(masterUrl)
    chroot = new Path(master, s"$storeDir/$folderName/" + conf.getAppId )
    fs = master.getFileSystem(new Configuration)
    if (!fs.exists(chroot)) {
      fs.mkdirs(chroot)
    }
    mkdir(subDirsPerAlluxio)
  }

  def delete(): Unit = {
    try {
      if (fs.exists(chroot)) {
        fs.delete(chroot, true)
      }
    } catch {
      case _ =>
        logWarning(s"$chroot has been deleted")
    } finally
      fs.close()
  }

  def mkdir(n: Int): Unit = {
    for (i <- 0 until n) {
      val path = new Path(chroot, i.toString + s"/$i")
      if (!fs.exists(path)) {
        fs.mkdirs(path)
      }
    }
  }

}
