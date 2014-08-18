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

package org.apache.spark.util

import java.io._

import org.apache.spark.Logging

/**
 * Various utility methods used by Spark for checking disks.
 */
private[spark] object DiskChecker extends Logging {

  def mkdirsWithExistsCheck(dir: File): Boolean = {
    if (dir.mkdir() || dir.exists()) {
      return true
    }
    var canonDir: File = null
    try {
      canonDir = dir.getCanonicalFile()
    } catch {
      case ie: IOException => return false
    }
    val parent = canonDir.getParent()
    (parent != null) && (mkdirsWithExistsCheck(new File(parent)) && (canonDir.mkdir() || canonDir.exists()))
  }

  def checkDir(dir: File): Boolean = {
    mkdirsWithExistsCheck(dir) && dir.isDirectory() && dir.canRead() && dir.canWrite()
  }
}
