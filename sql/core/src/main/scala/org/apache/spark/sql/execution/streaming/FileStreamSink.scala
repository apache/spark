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

package org.apache.spark.sql.execution.streaming

import java.nio.ByteBuffer

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.Logging
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.SQLContext

/**
 * A very simple sink that stores received data on the filesystem as a text file.
 * This is not atomic.
 */
class FileStreamSink(
    sqlContext: SQLContext,
    metadataPath: String,
    path: String) extends Sink with Logging {

  private def sparkContext = sqlContext.sparkContext
  private val fs = FileSystem.get(sparkContext.hadoopConfiguration)
  private val serializer = new JavaSerializer(sqlContext.sparkContext.conf).newInstance()

  override def currentOffset: Option[Offset] = {
    try {
      val buffer = new Array[Byte](10240)
      val stream = fs.open(new Path(metadataPath))
      val size = stream.read(buffer)
      val shrunk = ByteBuffer.wrap(buffer.take(size))
      Some(serializer.deserialize[Offset](shrunk))
    } catch {
      case _: java.io.FileNotFoundException =>
        None
    }
  }

  // TODO: this is not atomic.
  override def addBatch(batch: Batch): Unit = {
    batch.data.write.mode("append").text(path)
    val offset = serializer.serialize(batch.end)
    val stream = fs.create(new Path(metadataPath), true)
    stream.write(offset.array())
    stream.close()
    logInfo(s"Committed batch ${batch.end}")
  }
}
