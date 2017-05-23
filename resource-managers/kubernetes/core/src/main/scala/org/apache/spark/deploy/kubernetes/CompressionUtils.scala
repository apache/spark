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
package org.apache.spark.deploy.kubernetes

import java.io.{File, FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.common.io.Files
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream}
import org.apache.commons.compress.utils.CharsetNames
import org.apache.commons.io.IOUtils
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] object CompressionUtils extends Logging {
  // Defaults from TarArchiveOutputStream
  private val BLOCK_SIZE = 10240
  private val RECORD_SIZE = 512
  private val ENCODING = CharsetNames.UTF_8

  def writeTarGzipToStream(outputStream: OutputStream, paths: Iterable[String]): Unit = {
    Utils.tryWithResource(new GZIPOutputStream(outputStream)) { gzipping =>
      Utils.tryWithResource(new TarArchiveOutputStream(
          gzipping,
          BLOCK_SIZE,
          RECORD_SIZE,
          ENCODING)) { tarStream =>
        val usedFileNames = mutable.HashSet.empty[String]
        for (path <- paths) {
          val file = new File(path)
          if (!file.isFile) {
            throw new IllegalArgumentException(s"Cannot add $path to tarball; either does" +
              s" not exist or is a directory.")
          }
          var resolvedFileName = file.getName
          val extension = Files.getFileExtension(file.getName)
          val nameWithoutExtension = Files.getNameWithoutExtension(file.getName)
          var deduplicationCounter = 1
          while (usedFileNames.contains(resolvedFileName)) {
            val oldResolvedFileName = resolvedFileName
            resolvedFileName = s"$nameWithoutExtension-$deduplicationCounter.$extension"
            logWarning(s"File with name $oldResolvedFileName already exists. Trying to add" +
              s" with file name $resolvedFileName instead.")
            deduplicationCounter += 1
          }
          usedFileNames += resolvedFileName
          val tarEntry = new TarArchiveEntry(resolvedFileName)
          tarEntry.setSize(file.length());
          tarStream.putArchiveEntry(tarEntry)
          Utils.tryWithResource(new FileInputStream(file)) { fileInput =>
            IOUtils.copy(fileInput, tarStream)
          }
          tarStream.closeArchiveEntry()
        }
      }
    }
  }

  def unpackTarStreamToDirectory(inputStream: InputStream, outputDir: File): Seq[String] = {
    val paths = mutable.Buffer.empty[String]
    Utils.tryWithResource(new GZIPInputStream(inputStream)) { gzipped =>
      Utils.tryWithResource(new TarArchiveInputStream(
          gzipped,
          BLOCK_SIZE,
          RECORD_SIZE,
          ENCODING)) { tarInputStream =>
        var nextTarEntry = tarInputStream.getNextTarEntry
        while (nextTarEntry != null) {
          val outputFile = new File(outputDir, nextTarEntry.getName)
          Utils.tryWithResource(new FileOutputStream(outputFile)) { fileOutputStream =>
            IOUtils.copy(tarInputStream, fileOutputStream)
          }
          paths += outputFile.getAbsolutePath
          nextTarEntry = tarInputStream.getNextTarEntry
        }
      }
    }
    paths
  }
}
