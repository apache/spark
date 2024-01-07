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

package org.apache.spark.sql.execution.datasources.xml

import java.io.{File, RandomAccessFile}

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

private[xml] trait TestXmlData {
  protected def spark: SparkSession

  def sampledTestData: Dataset[String] = {
    spark
      .range(0, 100, 1)
      .map { index =>
        val predefinedSample = Set[Long](3, 18, 20, 24, 50, 60, 87, 99)
        if (predefinedSample.contains(index)) {
          index.toString
        } else {
          (index.toDouble + 0.1).toString
        }
      }(Encoders.STRING)
  }

  def withCorruptedFile(dir: File, format: String = "gz", numBytesToCorrupt: Int = 50)(
      f: File => Unit): Unit = {
    // find the targeted files and corrupt the first one
    val files = dir.listFiles().filter(file => file.isFile && file.getName.endsWith(format))
    val raf = new RandomAccessFile(files.head.getPath, "rw")

    // disable checksum verification
    import org.apache.hadoop.fs.Path
    val fs = new Path(dir.getPath).getFileSystem(spark.sessionState.newHadoopConf())
    fs.setVerifyChecksum(false)
    // delete crc files
    val crcFiles = dir.listFiles
      .filter(file => file.isFile && file.getName.endsWith("crc"))
    crcFiles.foreach { file =>
      assert(file.exists())
      file.delete()
      assert(!file.exists())
    }

    // corrupt the file
    val fileSize = raf.length()
    // avoid the last few bytes as it might contain crc
    raf.seek(fileSize - numBytesToCorrupt - 100)
    for (_ <- 1 to numBytesToCorrupt) {
      val randomByte = (Math.random() * 256).toByte
      raf.writeByte(randomByte)
    }
    raf.close()
    f(dir)
    fs.setVerifyChecksum(true)
  }
}
