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

package org.apache.spark.sql.sources.v2

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.util.{Collections, List => JList, Optional}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * A HDFS based transactional writable data source.
 * Each task writes data to `target/_temporary/jobId/$jobId-$partitionId-$attemptNumber`.
 * Each job moves files from `target/_temporary/jobId/` to `target`.
 */
class SimpleWritableDataSource extends DataSourceV2
  with ReadSupport
  with WriteSupport
  with SessionConfigSupport {

  protected def fullSchema() = new StructType().add("i", "long").add("j", "long")

  override def keyPrefix: String = "simpleWritableDataSource"

  class Reader(path: String, conf: Configuration) extends DataSourceReader {
    override def readSchema(): StructType = SimpleWritableDataSource.this.fullSchema()

    override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
      val dataPath = new Path(path)
      val fs = dataPath.getFileSystem(conf)
      if (fs.exists(dataPath)) {
        fs.listStatus(dataPath).filterNot { status =>
          val name = status.getPath.getName
          name.startsWith("_") || name.startsWith(".")
        }.map { f =>
          val serializableConf = new SerializableConfiguration(conf)
          new SimpleCSVInputPartitionReader(
            f.getPath.toUri.toString,
            serializableConf): InputPartition[InternalRow]
        }.toList.asJava
      } else {
        Collections.emptyList()
      }
    }
  }

  class Writer(jobId: String, path: String, conf: Configuration) extends DataSourceWriter {
    override def createWriterFactory(): DataWriterFactory[InternalRow] = {
      SimpleCounter.resetCounter
      new CSVDataWriterFactory(path, jobId, new SerializableConfiguration(conf))
    }

    override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
      SimpleCounter.increaseCounter
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      val finalPath = new Path(path)
      val jobPath = new Path(new Path(finalPath, "_temporary"), jobId)
      val fs = jobPath.getFileSystem(conf)
      try {
        for (file <- fs.listStatus(jobPath).map(_.getPath)) {
          val dest = new Path(finalPath, file.getName)
          if(!fs.rename(file, dest)) {
            throw new IOException(s"failed to rename($file, $dest)")
          }
        }
      } finally {
        fs.delete(jobPath, true)
      }
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
      val jobPath = new Path(new Path(path, "_temporary"), jobId)
      val fs = jobPath.getFileSystem(conf)
      fs.delete(jobPath, true)
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val path = new Path(options.get("path").get())
    val conf = SparkContext.getActive.get.hadoopConfiguration
    new Reader(path.toUri.toString, conf)
  }

  override def createWriter(
      jobId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions): Optional[DataSourceWriter] = {
    assert(!SparkContext.getActive.get.conf.getBoolean("spark.speculation", false))

    val path = new Path(options.get("path").get())
    val conf = SparkContext.getActive.get.hadoopConfiguration
    val fs = path.getFileSystem(conf)

    if (mode == SaveMode.ErrorIfExists) {
      if (fs.exists(path)) {
        throw new RuntimeException("data already exists.")
      }
    }
    if (mode == SaveMode.Ignore) {
      if (fs.exists(path)) {
        return Optional.empty()
      }
    }
    if (mode == SaveMode.Overwrite) {
      fs.delete(path, true)
    }

    val pathStr = path.toUri.toString
    Optional.of(new Writer(jobId, pathStr, conf))
  }
}

class SimpleCSVInputPartitionReader(path: String, conf: SerializableConfiguration)
  extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  @transient private var lines: Iterator[String] = _
  @transient private var currentLine: String = _
  @transient private var inputStream: FSDataInputStream = _

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    val filePath = new Path(path)
    val fs = filePath.getFileSystem(conf.value)
    inputStream = fs.open(filePath)
    lines = new BufferedReader(new InputStreamReader(inputStream))
      .lines().iterator().asScala
    this
  }

  override def next(): Boolean = {
    if (lines.hasNext) {
      currentLine = lines.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = InternalRow(currentLine.split(",").map(_.trim.toLong): _*)

  override def close(): Unit = {
    inputStream.close()
  }
}

private[v2] object SimpleCounter {
  private var count: Int = 0

  def increaseCounter: Unit = {
    count += 1
  }

  def getCounter: Int = {
    count
  }

  def resetCounter: Unit = {
    count = 0
  }
}

class CSVDataWriterFactory(path: String, jobId: String, conf: SerializableConfiguration)
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    val jobPath = new Path(new Path(path, "_temporary"), jobId)
    val filePath = new Path(jobPath, s"$jobId-$partitionId-$taskId")
    val fs = filePath.getFileSystem(conf.value)
    new CSVDataWriter(fs, filePath)
  }
}

class CSVDataWriter(fs: FileSystem, file: Path) extends DataWriter[InternalRow] {

  private val out = fs.create(file)

  override def write(record: InternalRow): Unit = {
    out.writeBytes(s"${record.getLong(0)},${record.getLong(1)}\n")
  }

  override def commit(): WriterCommitMessage = {
    out.close()
    null
  }

  override def abort(): Unit = {
    try {
      out.close()
    } finally {
      fs.delete(file, false)
    }
  }
}
