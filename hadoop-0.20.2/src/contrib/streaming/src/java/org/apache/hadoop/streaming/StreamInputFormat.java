/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.streaming;

import java.io.*;
import java.lang.reflect.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.*;

/** An input format that selects a RecordReader based on a JobConf property.
 *  This should be used only for non-standard record reader such as 
 *  StreamXmlRecordReader. For all other standard 
 *  record readers, the appropriate input format classes should be used.
 */
public class StreamInputFormat extends KeyValueTextInputFormat {

  @SuppressWarnings("unchecked")
  public RecordReader<Text, Text> getRecordReader(final InputSplit genericSplit,
                                      JobConf job, Reporter reporter) throws IOException {
    String c = job.get("stream.recordreader.class");
    if (c == null || c.indexOf("LineRecordReader") >= 0) {
      return super.getRecordReader(genericSplit, job, reporter);
    }

    // handling non-standard record reader (likely StreamXmlRecordReader) 
    FileSplit split = (FileSplit) genericSplit;
    LOG.info("getRecordReader start.....split=" + split);
    reporter.setStatus(split.toString());

    // Open the file and seek to the start of the split
    FileSystem fs = split.getPath().getFileSystem(job);
    FSDataInputStream in = fs.open(split.getPath());

    // Factory dispatch based on available params..
    Class readerClass;

    {
      readerClass = StreamUtil.goodClassOrNull(job, c, null);
      if (readerClass == null) {
        throw new RuntimeException("Class not found: " + c);
      }
    }

    Constructor ctor;
    try {
      ctor = readerClass.getConstructor(new Class[] { FSDataInputStream.class,
                                                      FileSplit.class, Reporter.class, JobConf.class, FileSystem.class });
    } catch (NoSuchMethodException nsm) {
      throw new RuntimeException(nsm);
    }

    RecordReader<Text, Text> reader;
    try {
      reader = (RecordReader<Text, Text>) ctor.newInstance(new Object[] { in, split,
                                                              reporter, job, fs });
    } catch (Exception nsm) {
      throw new RuntimeException(nsm);
    }
    return reader;
  }

}
