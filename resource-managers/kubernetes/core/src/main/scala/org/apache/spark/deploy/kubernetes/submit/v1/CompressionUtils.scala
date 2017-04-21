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
package org.apache.spark.deploy.kubernetes.submit.v1

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.common.io.Files
import org.apache.commons.codec.binary.Base64
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream}
import org.apache.commons.compress.utils.CharsetNames
import org.apache.commons.io.IOUtils
import scala.collection.mutable

import org.apache.spark.deploy.rest.kubernetes.v1.TarGzippedData
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ByteBufferOutputStream, Utils}

private[spark] object CompressionUtils extends Logging {
  // Defaults from TarArchiveOutputStream
  private val BLOCK_SIZE = 10240
  private val RECORD_SIZE = 512
  private val ENCODING = CharsetNames.UTF_8

  /**
   * Compresses all of the given paths into a gzipped-tar archive, returning the compressed data in
   * memory as an instance of {@link TarGzippedData}. The files are taken without consideration to
   * their original folder structure, and are added to the tar archive in a flat hierarchy.
   * Directories are not allowed, and duplicate file names are de-duplicated by appending a numeric
   * suffix to the file name, before the file extension. For example, if paths a/b.txt and b/b.txt
   * were provided, then the files added to the tar archive would be b.txt and b-1.txt.
   * @param paths A list of file paths to be archived
   * @return An in-memory representation of the compressed data.
   */
  def createTarGzip(paths: Iterable[String]): TarGzippedData = {
    val compressedBytesStream = Utils.tryWithResource(new ByteBufferOutputStream()) { raw =>
      Utils.tryWithResource(new GZIPOutputStream(raw)) { gzipping =>
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
            val tarEntry = new TarArchiveEntry(file, resolvedFileName)
            tarStream.putArchiveEntry(tarEntry)
            Utils.tryWithResource(new FileInputStream(file)) { fileInput =>
              IOUtils.copy(fileInput, tarStream)
            }
            tarStream.closeArchiveEntry()
          }
        }
      }
      raw
    }
    val compressedAsBase64 = Base64.encodeBase64String(compressedBytesStream.toByteBuffer.array)
    TarGzippedData(
      dataBase64 = compressedAsBase64,
      blockSize = BLOCK_SIZE,
      recordSize = RECORD_SIZE,
      encoding = ENCODING
    )
  }

  /**
   * Decompresses the provided tar archive to a directory.
   * @param compressedData In-memory representation of the compressed data, ideally created via
   *                       {@link createTarGzip}.
   * @param rootOutputDir  Directory to write the output files to. All files from the tarball
   *                       are written here in a flat hierarchy.
   * @return List of file paths for each file that was unpacked from the archive.
   */
  def unpackAndWriteCompressedFiles(
      compressedData: TarGzippedData,
      rootOutputDir: File): Seq[String] = {
    val paths = mutable.Buffer.empty[String]
    val compressedBytes = Base64.decodeBase64(compressedData.dataBase64)
    if (!rootOutputDir.exists) {
      if (!rootOutputDir.mkdirs) {
        throw new IllegalStateException(s"Failed to create output directory for unpacking" +
          s" files at ${rootOutputDir.getAbsolutePath}")
      }
    } else if (rootOutputDir.isFile) {
      throw new IllegalArgumentException(s"Root dir for writing decompressed files: " +
         s"${rootOutputDir.getAbsolutePath} exists and is not a directory.")
    }
    Utils.tryWithResource(new ByteArrayInputStream(compressedBytes)) { compressedBytesStream =>
      Utils.tryWithResource(new GZIPInputStream(compressedBytesStream)) { gzipped =>
        Utils.tryWithResource(new TarArchiveInputStream(
            gzipped,
            compressedData.blockSize,
            compressedData.recordSize,
            compressedData.encoding)) { tarInputStream =>
          var nextTarEntry = tarInputStream.getNextTarEntry
          while (nextTarEntry != null) {
            val outputFile = new File(rootOutputDir, nextTarEntry.getName)
            Utils.tryWithResource(new FileOutputStream(outputFile)) { fileOutputStream =>
              IOUtils.copy(tarInputStream, fileOutputStream)
            }
            paths += outputFile.getAbsolutePath
            nextTarEntry = tarInputStream.getNextTarEntry
          }
        }
      }
    }
    paths.toSeq
  }
}
