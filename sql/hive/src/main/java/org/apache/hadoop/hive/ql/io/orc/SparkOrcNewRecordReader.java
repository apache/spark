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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * This is based on hive-exec-1.2.1
 * {@link org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat.OrcRecordReader}.
 * This class exposes getObjectInspector which can be used for reducing
 * NameNode calls in OrcRelation.
 */
public class SparkOrcNewRecordReader extends
    org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct> {
  private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
  private final int numColumns;
  OrcStruct value;
  private float progress = 0.0f;
  private ObjectInspector objectInspector;

  public SparkOrcNewRecordReader(Reader file, Configuration conf,
      long offset, long length) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    value = new OrcStruct(numColumns);
    this.reader = OrcInputFormat.createReaderFromFile(file, conf, offset,
        length);
    this.objectInspector = file.getObjectInspector();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException,
      InterruptedException {
    return NullWritable.get();
  }

  @Override
  public OrcStruct getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return progress;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader.hasNext()) {
      reader.next(value);
      progress = reader.getProgress();
      return true;
    } else {
      return false;
    }
  }

  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }
}
