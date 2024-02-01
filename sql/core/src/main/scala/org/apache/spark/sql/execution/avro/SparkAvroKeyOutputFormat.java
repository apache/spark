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

package org.apache.spark.sql.execution.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;

// A variant of `AvroKeyOutputFormat`, which is used to inject the custom `RecordWriterFactory` so
// that we can set avro file metadata.
class SparkAvroKeyOutputFormat extends AvroKeyOutputFormat<GenericRecord> {
  SparkAvroKeyOutputFormat(Map<String, String> metadata) {
    super(new SparkRecordWriterFactory(metadata));
  }

  static class SparkRecordWriterFactory extends RecordWriterFactory<GenericRecord> {
    private final Map<String, String> metadata;
    SparkRecordWriterFactory(Map<String, String> metadata) {
      this.metadata = metadata;
    }

    @Override
    protected RecordWriter<AvroKey<GenericRecord>, NullWritable> create(
        Schema writerSchema,
        GenericData dataModel,
        CodecFactory compressionCodec,
        OutputStream outputStream,
        int syncInterval) throws IOException {
      return new SparkAvroKeyRecordWriter<>(
        writerSchema, dataModel, compressionCodec, outputStream, syncInterval, metadata);
    }
  }
}

