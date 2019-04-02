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
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

/**
 * A HDFS based transactional writable data source.
 * Each task writes data to `target/_temporary/uniqueId/$jobId-$partitionId-$attemptNumber`.
 * Each job moves files from `target/_temporary/uniqueId/` to `target`.
 */
class SimpleWritableDataSource extends TableProvider with SessionConfigSupport {

  private val tableSchema = new StructType().add("i", "long").add("j", "long")

  override def keyPrefix: String = "simpleWritableDataSource"

  class MyScanBuilder(path: String, conf: Configuration) extends SimpleScanBuilder {
    override def planInputPartitions(): Array[InputPartition] = {
      val dataPath = new Path(path)
      val fs = dataPath.getFileSystem(conf)
      if (fs.exists(dataPath)) {
        fs.listStatus(dataPath).filterNot { status =>
          val name = status.getPath.getName
          name.startsWith("_") || name.startsWith(".")
        }.map { f =>
          CSVInputPartitionReader(f.getPath.toUri.toString)
        }.toArray
      } else {
        Array.empty
      }
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      val serializableConf = new SerializableConfiguration(conf)
      new CSVReaderFactory(serializableConf)
    }

    override def readSchema(): StructType = tableSchema
  }

  class MyWriteBuilder(path: String) extends WriteBuilder with SupportsSaveMode {
    private var queryId: String = _
    private var mode: SaveMode = _

    override def withQueryId(queryId: String): WriteBuilder = {
      this.queryId = queryId
      this
    }

    override def mode(mode: SaveMode): WriteBuilder = {
      this.mode = mode
      this
    }

    override def buildForBatch(): BatchWrite = {
      assert(mode != null)

      val hadoopPath = new Path(path)
      val hadoopConf = SparkContext.getActive.get.hadoopConfiguration
      val fs = hadoopPath.getFileSystem(hadoopConf)

      if (mode == SaveMode.ErrorIfExists) {
        if (fs.exists(hadoopPath)) {
          throw new RuntimeException("data already exists.")
        }
      }
      if (mode == SaveMode.Ignore) {
        if (fs.exists(hadoopPath)) {
          return null
        }
      }
      if (mode == SaveMode.Overwrite) {
        fs.delete(hadoopPath, true)
      }

      val pathStr = hadoopPath.toUri.toString
      new MyBatchWrite(queryId, pathStr, hadoopConf)
    }
  }

  class MyBatchWrite(queryId: String, path: String, conf: Configuration) extends BatchWrite {
    override def createBatchWriterFactory(): DataWriterFactory = {
      SimpleCounter.resetCounter
      new CSVDataWriterFactory(path, queryId, new SerializableConfiguration(conf))
    }

    override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
      SimpleCounter.increaseCounter
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      val finalPath = new Path(path)
      val jobPath = new Path(new Path(finalPath, "_temporary"), queryId)
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
      val jobPath = new Path(new Path(path, "_temporary"), queryId)
      val fs = jobPath.getFileSystem(conf)
      fs.delete(jobPath, true)
    }
  }

  class MyTable(options: CaseInsensitiveStringMap)
    extends SimpleBatchTable with SupportsWrite {

    private val path = options.get("path")
    private val conf = SparkContext.getActive.get.hadoopConfiguration

    override def schema(): StructType = tableSchema

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder(new Path(path).toUri.toString, conf)
    }

    override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
      new MyWriteBuilder(path)
    }

    override def capabilities(): util.Set[TableCapability] =
      Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new MyTable(options)
  }
}

case class CSVInputPartitionReader(path: String) extends InputPartition

class CSVReaderFactory(conf: SerializableConfiguration)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val path = partition.asInstanceOf[CSVInputPartitionReader].path
    val filePath = new Path(path)
    val fs = filePath.getFileSystem(conf.value)

    new PartitionReader[InternalRow] {
      private val inputStream = fs.open(filePath)
      private val lines = new BufferedReader(new InputStreamReader(inputStream))
        .lines().iterator().asScala

      private var currentLine: String = _

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
  extends DataWriterFactory {

  override def createWriter(
      partitionId: Int,
      taskId: Long): DataWriter[InternalRow] = {
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
