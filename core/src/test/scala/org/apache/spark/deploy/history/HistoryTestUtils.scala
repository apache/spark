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
package org.apache.spark.deploy.history

import java.io.{BufferedOutputStream, File, FileOutputStream, InputStream}
import java.util.zip.ZipInputStream

object HistoryTestUtils {

  def unzipToDir(inputStream: InputStream, dir: File): Unit = {
    val unzipStream = new ZipInputStream(inputStream)
    try {
      val buffer = new Array[Byte](64 * 1024 * 1024)
      var nextEntry = unzipStream.getNextEntry
      while (nextEntry != null) {
        var file: File = null
        if (nextEntry.isDirectory) {
          val newDir = new File(dir, nextEntry.getName)
          newDir.mkdirs()
        } else {
          val splits = nextEntry.getName.split("/")
          if (splits.length == 2) {
            file = new File(new File(dir, splits(0)), splits(1))
          } else {
            file = new File(dir, nextEntry.getName)
          }
          val outputStream = new BufferedOutputStream(new FileOutputStream(file))
          var dataRemains = true
          while (dataRemains) {
            val read = unzipStream.read(buffer)
            if (read != -1) outputStream.write(buffer, 0, read)
            else dataRemains = false
          }
          outputStream.close()
        }
        nextEntry = unzipStream.getNextEntry
      }
    } finally {
      unzipStream.close()
    }
  }
}
