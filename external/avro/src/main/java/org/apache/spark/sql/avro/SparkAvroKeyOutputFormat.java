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

package org.apache.spark.sql.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.Syncable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

// A variant of `AvroKeyOutputFormat`, which is used to inject the custom `RecordWriterFactory` so
// that we can set avro file metadata.
public class SparkAvroKeyOutputFormat extends AvroKeyOutputFormat<InternalRow> {

  public SparkAvroKeyOutputFormat(Map<String, String> metadata, StructType dataSchema) {
    super(new SparkRecordWriterFactory(metadata, dataSchema));
  }

  static class SparkRecordWriterFactory extends RecordWriterFactory<InternalRow> {
    private final Map<String, String> metadata;
    private final StructType dataSchema;
    SparkRecordWriterFactory(Map<String, String> metadata, StructType dataSchema) {
      this.metadata = metadata;
      this.dataSchema = dataSchema;
    }

    protected RecordWriter<AvroKey<InternalRow>, NullWritable> create(
        Schema writerSchema,
        GenericData dataModel_Ignored,
        CodecFactory compressionCodec,
        OutputStream outputStream,
        int syncInterval) throws IOException {
      return new SparkAvroKeyRecordWriter(
        writerSchema, dataSchema, compressionCodec, outputStream, syncInterval, metadata);
    }
  }
}

// This a fork of org.apache.avro.mapreduce.AvroKeyRecordWriter, in order to set file metadata.
class SparkAvroKeyRecordWriter<T> extends RecordWriter<AvroKey<T>, NullWritable>
    implements Syncable {

  private final DataFileWriter<T> mAvroFileWriter;

  SparkAvroKeyRecordWriter(
      Schema writerSchema,
      StructType dataSchema,
      CodecFactory compressionCodec,
      OutputStream outputStream,
      int syncInterval,
      Map<String, String> metadata) throws IOException {
    DatumWriter datumWriter = new SparkAvroDatumWriter(writerSchema, dataSchema);
    this.mAvroFileWriter = new DataFileWriter(datumWriter);
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      this.mAvroFileWriter.setMeta(entry.getKey(), entry.getValue());
    }
    this.mAvroFileWriter.setCodec(compressionCodec);
    this.mAvroFileWriter.setSyncInterval(syncInterval);
    this.mAvroFileWriter.create(writerSchema, outputStream);
  }

  public void write(AvroKey<T> record, NullWritable ignore) throws IOException {
    this.mAvroFileWriter.append(record.datum());
  }

  public void close(TaskAttemptContext context) throws IOException {
    this.mAvroFileWriter.close();
  }

  public long sync() throws IOException {
    return this.mAvroFileWriter.sync();
  }
}
