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

package org.apache.hadoop.contrib.index.example;

import java.io.IOException;

import org.apache.hadoop.contrib.index.mapred.DocumentID;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An InputFormat for LineDoc for plain text files where each line is a doc.
 */
public class LineDocInputFormat extends
    FileInputFormat<DocumentID, LineDocTextAndOp> {

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.FileInputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit, org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  public RecordReader<DocumentID, LineDocTextAndOp> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new LineDocRecordReader(job, (FileSplit) split);
  }

}
