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

package test.org.apache.spark.sql.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.SimpleCounter;
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

/**
 * A HDFS based transactional writable data source which is implemented by java.
 * Each task writes data to `target/_temporary/uniqueId/$jobId-$partitionId-$attemptNumber`.
 * Each job moves files from `target/_temporary/uniqueId/` to `target`.
 */
public class JavaSimpleWritableDataSource implements TestingV2Source {

  static class MyScanBuilder extends JavaSimpleScanBuilder {

    private final String path;
    private final Configuration conf;

    MyScanBuilder(String path, Configuration conf) {
      this.path = path;
      this.conf = conf;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      Path dataPath = new Path(this.path);
      try {
        FileSystem fs = dataPath.getFileSystem(conf);
        if (fs.exists(dataPath)) {
          return Arrays.stream(fs.listStatus(dataPath))
              .filter(
                  status -> {
                    String name = status.getPath().getName();
                    return !name.startsWith("_") && !name.startsWith(".");
                  })
              .map(f -> new JavaCSVInputPartitionReader(f.getPath().toUri().toString()))
              .toArray(InputPartition[]::new);
        } else {
          return new InputPartition[0];
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      SerializableConfiguration serializableConf = new SerializableConfiguration(conf);
      return new JavaCSVReaderFactory(serializableConf);
    }
  }

  static class MyWriteBuilder implements WriteBuilder, SupportsTruncate {

    private final String path;
    private final String queryId;
    private boolean needTruncate = false;

    MyWriteBuilder(String path, LogicalWriteInfo info) {
      this.path = path;
      this.queryId = info.queryId();
    }

    @Override
    public WriteBuilder truncate() {
      this.needTruncate = true;
      return this;
    }

    @Override
    public Write build() {
      return new MyWrite(path, queryId, needTruncate);
    }
  }

  static class MyWrite implements Write {

    private final String path;
    private final String queryId;
    private final boolean needTruncate;

    MyWrite(String path, String queryId, boolean needTruncate) {
      this.path = path;
      this.queryId = queryId;
      this.needTruncate = needTruncate;
    }

    @Override
    public BatchWrite toBatch() {
      Path hadoopPath = new Path(path);
      Configuration hadoopConf = SparkHadoopUtil.get().conf();
      try {
        FileSystem fs = hadoopPath.getFileSystem(hadoopConf);
        if (needTruncate) {
          fs.delete(hadoopPath, true);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      String pathStr = hadoopPath.toUri().toString();
      return new MyBatchWrite(queryId, pathStr, hadoopConf);
    }
  }

  static class MyBatchWrite implements BatchWrite {

    private final String queryId;
    private final String path;
    private final Configuration conf;

    MyBatchWrite(String queryId, String path, Configuration conf) {
      this.queryId = queryId;
      this.path = path;
      this.conf = conf;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      SimpleCounter.resetCounter();
      return new JavaCSVDataWriterFactory(path, queryId, new SerializableConfiguration(conf));
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
      SimpleCounter.increaseCounter();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      Path finalPath = new Path(this.path);
      Path jobPath = new Path(new Path(finalPath, "_temporary"), queryId);
      try {
        FileSystem fs = jobPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(jobPath);
        try {
          for (FileStatus status : fileStatuses) {
            Path file = status.getPath();
            Path dest = new Path(finalPath, file.getName());
            if (!fs.rename(file, dest)) {
              throw new IOException(String.format("failed to rename(%s, %s)", file, dest));
            }
          }
        } finally {
          fs.delete(jobPath, true);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      try {
        Path jobPath = new Path(new Path(this.path, "_temporary"), queryId);
        FileSystem fs = jobPath.getFileSystem(conf);
        fs.delete(jobPath, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class MyTable extends JavaSimpleBatchTable implements SupportsWrite {

    private final String path;
    private final Configuration conf = SparkHadoopUtil.get().conf();

    MyTable(CaseInsensitiveStringMap options) {
      this.path = options.get("path");
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return new MyScanBuilder(new Path(path).toUri().toString(), conf);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      return new MyWriteBuilder(path, info);
    }

    @Override
    public Set<TableCapability> capabilities() {
      return EnumSet.of(
          TableCapability.BATCH_READ,
          TableCapability.BATCH_WRITE,
          TableCapability.TRUNCATE);
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new MyTable(options);
  }

  static class JavaCSVInputPartitionReader implements InputPartition {

    private String path;

    JavaCSVInputPartitionReader(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }
  }

  static class JavaCSVReaderFactory implements PartitionReaderFactory {

    private final SerializableConfiguration conf;

    JavaCSVReaderFactory(SerializableConfiguration conf) {
      this.conf = conf;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      String path = ((JavaCSVInputPartitionReader) partition).getPath();
      Path filePath = new Path(path);
      try {
        FileSystem fs = filePath.getFileSystem(conf.value());
        return new PartitionReader<InternalRow>() {
          private final FSDataInputStream inputStream = fs.open(filePath);
          private final Iterator<String> lines =
              new BufferedReader(new InputStreamReader(inputStream)).lines().iterator();
          private String currentLine = "";

          @Override
          public boolean next() {
            if (lines.hasNext()) {
              currentLine = lines.next();
              return true;
            } else {
              return false;
            }
          }

          @Override
          public InternalRow get() {
            Object[] objects =
                Arrays.stream(currentLine.split(","))
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .toArray();
            return new GenericInternalRow(objects);
          }

          @Override
          public void close() throws IOException {
            inputStream.close();
          }
        };
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class JavaCSVDataWriterFactory implements DataWriterFactory {

    private final String path;
    private final String jobId;
    private final SerializableConfiguration conf;

    JavaCSVDataWriterFactory(String path, String jobId, SerializableConfiguration conf) {
      this.path = path;
      this.jobId = jobId;
      this.conf = conf;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      try {
        Path jobPath = new Path(new Path(path, "_temporary"), jobId);
        Path filePath = new Path(jobPath, String.format("%s-%d-%d", jobId, partitionId, taskId));
        FileSystem fs = filePath.getFileSystem(conf.value());
        return new JavaCSVDataWriter(fs, filePath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class JavaCSVDataWriter implements DataWriter<InternalRow> {

    private final FileSystem fs;
    private final Path file;
    private final FSDataOutputStream out;

    JavaCSVDataWriter(FileSystem fs, Path file) throws IOException {
      this.fs = fs;
      this.file = file;
      out = fs.create(file);
    }

    @Override
    public void write(InternalRow record) throws IOException {
      out.writeBytes(String.format("%d,%d\n", record.getInt(0), record.getInt(1)));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      out.close();
      return null;
    }

    @Override
    public void abort() throws IOException {
      try {
        out.close();
      } finally {
        fs.delete(file, false);
      }
    }

    @Override
    public void close() {
    }
  }
}
