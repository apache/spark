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
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataSourceV2Reader, ReadTask}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * A HDFS based transactional writable data source.
 * Each task writes data to `target/_temporary/jobId/$jobId-$partitionId-$attemptNumber`.
 * Each job moves files from `target/_temporary/jobId/` to `target`.
 */
class SimpleWritableDataSource extends DataSourceV2 with ReadSupport with WriteSupport {

  private val schema = new StructType().add("i", "long").add("j", "long")

  class Reader(path: String, conf: Configuration) extends DataSourceV2Reader {
    override def readSchema(): StructType = schema

    override def createReadTasks(): JList[ReadTask[Row]] = {
      val dataPath = new Path(path)
      val fs = dataPath.getFileSystem(conf)
      if (fs.exists(dataPath)) {
        fs.listStatus(dataPath).filterNot { status =>
          val name = status.getPath.getName
          name.startsWith("_") || name.startsWith(".")
        }.map { f =>
          val serializableConf = new SerializableConfiguration(conf)
          new SimpleCSVReadTask(f.getPath.toUri.toString, serializableConf): ReadTask[Row]
        }.toList.asJava
      } else {
        Collections.emptyList()
      }
    }
  }

  class Writer(jobId: String, path: String, conf: Configuration) extends DataSourceV2Writer {
    override def createWriterFactory(): DataWriterFactory[Row] = {
      new SimpleCSVDataWriterFactory(path, jobId, new SerializableConfiguration(conf))
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

  class InternalRowWriter(jobId: String, path: String, conf: Configuration)
    extends Writer(jobId, path, conf) with SupportsWriteInternalRow {

    override def createWriterFactory(): DataWriterFactory[Row] = {
      throw new IllegalArgumentException("not expected!")
    }

    override def createInternalRowWriterFactory(): DataWriterFactory[InternalRow] = {
      new InternalRowCSVDataWriterFactory(path, jobId, new SerializableConfiguration(conf))
    }
  }

  override def createReader(options: DataSourceV2Options): DataSourceV2Reader = {
    val path = new Path(options.get("path").get())
    val conf = SparkContext.getActive.get.hadoopConfiguration
    new Reader(path.toUri.toString, conf)
  }

  override def createWriter(
      jobId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceV2Options): Optional[DataSourceV2Writer] = {
    assert(DataType.equalsStructurally(schema.asNullable, this.schema.asNullable))
    assert(!SparkContext.getActive.get.conf.getBoolean("spark.speculation", false))

    val path = new Path(options.get("path").get())
    val internal = options.get("internal").isPresent
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

    Optional.of(createWriter(jobId, path, conf, internal))
  }

  private def createWriter(
      jobId: String, path: Path, conf: Configuration, internal: Boolean): DataSourceV2Writer = {
    val pathStr = path.toUri.toString
    if (internal) {
      new InternalRowWriter(jobId, pathStr, conf)
    } else {
      new Writer(jobId, pathStr, conf)
    }
  }
}

class SimpleCSVReadTask(path: String, conf: SerializableConfiguration)
  extends ReadTask[Row] with DataReader[Row] {

  @transient private var lines: Iterator[String] = _
  @transient private var currentLine: String = _
  @transient private var inputStream: FSDataInputStream = _

  override def createDataReader(): DataReader[Row] = {
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

  override def get(): Row = Row(currentLine.split(",").map(_.trim.toLong): _*)

  override def close(): Unit = {
    inputStream.close()
  }
}

class SimpleCSVDataWriterFactory(path: String, jobId: String, conf: SerializableConfiguration)
  extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    val jobPath = new Path(new Path(path, "_temporary"), jobId)
    val filePath = new Path(jobPath, s"$jobId-$partitionId-$attemptNumber")
    val fs = filePath.getFileSystem(conf.value)
    new SimpleCSVDataWriter(fs, filePath)
  }
}

class SimpleCSVDataWriter(fs: FileSystem, file: Path) extends DataWriter[Row] {

  private val out = fs.create(file)

  override def write(record: Row): Unit = {
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

class InternalRowCSVDataWriterFactory(path: String, jobId: String, conf: SerializableConfiguration)
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[InternalRow] = {
    val jobPath = new Path(new Path(path, "_temporary"), jobId)
    val filePath = new Path(jobPath, s"$jobId-$partitionId-$attemptNumber")
    val fs = filePath.getFileSystem(conf.value)
    new InternalRowCSVDataWriter(fs, filePath)
  }
}

class InternalRowCSVDataWriter(fs: FileSystem, file: Path) extends DataWriter[InternalRow] {

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
