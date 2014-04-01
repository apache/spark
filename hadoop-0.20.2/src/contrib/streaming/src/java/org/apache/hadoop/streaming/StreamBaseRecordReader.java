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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.*;

/** 
 * Shared functionality for hadoopStreaming formats.
 * A custom reader can be defined to be a RecordReader with the constructor below
 * and is selected with the option bin/hadoopStreaming -inputreader ...
 * @see StreamXmlRecordReader 
 */
public abstract class StreamBaseRecordReader implements RecordReader<Text, Text> {

  protected static final Log LOG = LogFactory.getLog(StreamBaseRecordReader.class.getName());

  // custom JobConf properties for this class are prefixed with this namespace
  final static String CONF_NS = "stream.recordreader.";

  public StreamBaseRecordReader(FSDataInputStream in, FileSplit split, Reporter reporter,
                                JobConf job, FileSystem fs) throws IOException {
    in_ = in;
    split_ = split;
    start_ = split_.getStart();
    length_ = split_.getLength();
    end_ = start_ + length_;
    splitName_ = split_.getPath().getName();
    reporter_ = reporter;
    job_ = job;
    fs_ = fs;

    statusMaxRecordChars_ = job_.getInt(CONF_NS + "statuschars", 200);
  }

  /// RecordReader API

  /** Read a record. Implementation should call numRecStats at the end
   */
  public abstract boolean next(Text key, Text value) throws IOException;

  /** Returns the current position in the input. */
  public synchronized long getPos() throws IOException {
    return in_.getPos();
  }

  /** Close this to future operations.*/
  public synchronized void close() throws IOException {
    in_.close();
  }

  public float getProgress() throws IOException {
    if (end_ == start_) {
      return 1.0f;
    } else {
      return ((float)(in_.getPos() - start_)) / ((float)(end_ - start_));
    }
  }
  
  public Text createKey() {
    return new Text();
  }

  public Text createValue() {
    return new Text();
  }

  /// StreamBaseRecordReader API

  /** Implementation should seek forward in_ to the first byte of the next record.
   *  The initial byte offset in the stream is arbitrary.
   */
  public abstract void seekNextRecordBoundary() throws IOException;

  void numRecStats(byte[] record, int start, int len) throws IOException {
    numRec_++;
    if (numRec_ == nextStatusRec_) {
      String recordStr = new String(record, start, Math.min(len, statusMaxRecordChars_), "UTF-8");
      nextStatusRec_ += 100;//*= 10;
      String status = getStatus(recordStr);
      LOG.info(status);
      reporter_.setStatus(status);
    }
  }

  long lastMem = 0;

  String getStatus(CharSequence record) {
    long pos = -1;
    try {
      pos = getPos();
    } catch (IOException io) {
    }
    String recStr;
    if (record.length() > statusMaxRecordChars_) {
      recStr = record.subSequence(0, statusMaxRecordChars_) + "...";
    } else {
      recStr = record.toString();
    }
    String unqualSplit = split_.getPath().getName() + ":" +
                         split_.getStart() + "+" + split_.getLength();
    String status = "HSTR " + StreamUtil.HOST + " " + numRec_ + ". pos=" + pos + " " + unqualSplit
      + " Processing record=" + recStr;
    status += " " + splitName_;
    return status;
  }

  FSDataInputStream in_;
  FileSplit split_;
  long start_;
  long end_;
  long length_;
  String splitName_;
  Reporter reporter_;
  JobConf job_;
  FileSystem fs_;
  int numRec_ = 0;
  int nextStatusRec_ = 1;
  int statusMaxRecordChars_;

}
