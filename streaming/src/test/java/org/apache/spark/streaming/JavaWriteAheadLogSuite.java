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

package org.apache.spark.streaming;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.util.WriteAheadLog;
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle;
import org.apache.spark.streaming.util.WriteAheadLogUtils;

import org.junit.Test;
import org.junit.Assert;

class JavaWriteAheadLogSuiteHandle extends WriteAheadLogRecordHandle {
  int index = -1;
  public JavaWriteAheadLogSuiteHandle(int idx) {
    index = idx;
  }
}

public class JavaWriteAheadLogSuite extends WriteAheadLog {

  class Record {
    long time;
    int index;
    ByteBuffer buffer;

    public Record(long tym, int idx, ByteBuffer buf) {
      index = idx;
      time = tym;
      buffer = buf;
    }
  }
  private int index = -1;
  private ArrayList<Record> records = new ArrayList<Record>();


  // Methods for WriteAheadLog
  @Override
  public WriteAheadLogRecordHandle write(java.nio.ByteBuffer record, long time) {
    index += 1;
    records.add(new org.apache.spark.streaming.JavaWriteAheadLogSuite.Record(time, index, record));
    return new JavaWriteAheadLogSuiteHandle(index);
  }

  @Override
  public java.nio.ByteBuffer read(WriteAheadLogRecordHandle handle) {
    if (handle instanceof JavaWriteAheadLogSuiteHandle) {
      int reqdIndex = ((JavaWriteAheadLogSuiteHandle) handle).index;
      for (Record record: records) {
        if (record.index == reqdIndex) {
          return record.buffer;
        }
      }
    }
    return null;
  }

  @Override
  public java.util.Iterator<java.nio.ByteBuffer> readAll() {
    Collection<ByteBuffer> buffers = CollectionUtils.collect(records, new Transformer() {
      @Override
      public Object transform(Object input) {
        return ((Record) input).buffer;
      }
    });
    return buffers.iterator();
  }

  @Override
  public void clean(long threshTime, boolean waitForCompletion) {
    for (int i = 0; i < records.size(); i++) {
      if (records.get(i).time < threshTime) {
        records.remove(i);
        i--;
      }
    }
  }

  @Override
  public void close() {
    records.clear();
  }

  @Test
  public void testCustomWAL() {
    SparkConf conf = new SparkConf();
    conf.set("spark.streaming.driver.writeAheadLog.class", JavaWriteAheadLogSuite.class.getName());
    WriteAheadLog wal = WriteAheadLogUtils.createLogForDriver(conf, null, null);

    String data1 = "data1";
    WriteAheadLogRecordHandle handle = wal.write(ByteBuffer.wrap(data1.getBytes()), 1234);
    Assert.assertTrue(handle instanceof JavaWriteAheadLogSuiteHandle);
    Assert.assertTrue(new String(wal.read(handle).array()).equals(data1));

    wal.write(ByteBuffer.wrap("data2".getBytes()), 1235);
    wal.write(ByteBuffer.wrap("data3".getBytes()), 1236);
    wal.write(ByteBuffer.wrap("data4".getBytes()), 1237);
    wal.clean(1236, false);

    java.util.Iterator<java.nio.ByteBuffer> dataIterator = wal.readAll();
    ArrayList<String> readData = new ArrayList<String>();
    while (dataIterator.hasNext()) {
      readData.add(new String(dataIterator.next().array()));
    }
    Assert.assertTrue(readData.equals(Arrays.asList("data3", "data4")));
  }
}
