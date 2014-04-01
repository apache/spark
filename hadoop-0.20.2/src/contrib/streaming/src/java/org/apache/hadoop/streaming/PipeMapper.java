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
import java.net.URLDecoder;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.streaming.io.InputWriter;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.streaming.io.TextInputWriter;
import org.apache.hadoop.util.StringUtils;

/** A generic Mapper bridge.
 *  It delegates operations to an external program via stdin and stdout.
 */
public class PipeMapper extends PipeMapRed implements Mapper {

  private boolean ignoreKey = false;
  private boolean skipping = false;

  private byte[] mapOutputFieldSeparator;
  private byte[] mapInputFieldSeparator;
  private int numOfMapOutputKeyFields = 1;
  
  String getPipeCommand(JobConf job) {
    String str = job.get("stream.map.streamprocessor");
    if (str == null) {
      return str;
    }
    try {
      return URLDecoder.decode(str, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {
      System.err.println("stream.map.streamprocessor in jobconf not found");
      return null;
    }
  }

  boolean getDoPipe() {
    return true;
  }
  
  public void configure(JobConf job) {
    super.configure(job);
    //disable the auto increment of the counter. For streaming, no of 
    //processed records could be different(equal or less) than the no of 
    //records input.
    SkipBadRecords.setAutoIncrMapperProcCount(job, false);
    skipping = job.getBoolean("mapred.skip.on", false);
    if (mapInputWriterClass_.getCanonicalName().equals(TextInputWriter.class.getCanonicalName())) {
      String inputFormatClassName = job.getClass("mapred.input.format.class", TextInputFormat.class).getCanonicalName();
      ignoreKey = job.getBoolean("stream.map.input.ignoreKey", 
        inputFormatClassName.equals(TextInputFormat.class.getCanonicalName()));
    }
    
    try {
      mapOutputFieldSeparator = job.get("stream.map.output.field.separator", "\t").getBytes("UTF-8");
      mapInputFieldSeparator = job.get("stream.map.input.field.separator", "\t").getBytes("UTF-8");
      numOfMapOutputKeyFields = job.getInt("stream.num.map.output.key.fields", 1);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("The current system does not support UTF-8 encoding!", e);
    }
  }

  // Do NOT declare default constructor
  // (MapRed creates it reflectively)

  public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
    if (outerrThreadsThrowable != null) {
      mapRedFinished();
      throw new IOException ("MROutput/MRErrThread failed:"
                             + StringUtils.stringifyException(
                                                              outerrThreadsThrowable));
    }
    try {
      // 1/4 Hadoop in
      numRecRead_++;
      maybeLogRecord();
      if (debugFailDuring_ && numRecRead_ == 3) {
        throw new IOException("debugFailDuring_");
      }

      // 2/4 Hadoop to Tool
      if (numExceptions_ == 0) {
        if (!this.ignoreKey) {
          inWriter_.writeKey(key);
        }
        inWriter_.writeValue(value);
        if(skipping) {
          //flush the streams on every record input if running in skip mode
          //so that we don't buffer other records surrounding a bad record. 
          clientOut_.flush();
        }
      } else {
        numRecSkipped_++;
      }
    } catch (IOException io) {
      numExceptions_++;
      if (numExceptions_ > 1 || numRecWritten_ < minRecWrittenToEnableSkip_) {
        // terminate with failure
        String msg = logFailure(io);
        appendLogToJobLog("failure");
        mapRedFinished();
        throw new IOException(msg);
      } else {
        // terminate with success:
        // swallow input records although the stream processor failed/closed
      }
    }
  }

  public void close() {
    appendLogToJobLog("success");
    mapRedFinished();
  }

  @Override
  public byte[] getInputSeparator() {
    return mapInputFieldSeparator;
  }

  @Override
  public byte[] getFieldSeparator() {
    return mapOutputFieldSeparator;
  }

  @Override
  public int getNumOfKeyFields() {
    return numOfMapOutputKeyFields;
  }

  @Override
  InputWriter createInputWriter() throws IOException {
    return super.createInputWriter(mapInputWriterClass_);
  }

  @Override
  OutputReader createOutputReader() throws IOException {
    return super.createOutputReader(mapOutputReaderClass_);
  }

}
