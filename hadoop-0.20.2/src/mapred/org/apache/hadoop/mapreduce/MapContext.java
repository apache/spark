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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * The context that is given to the {@link Mapper}.
 * @param <KEYIN> the key input type to the Mapper
 * @param <VALUEIN> the value input type to the Mapper
 * @param <KEYOUT> the key output type from the Mapper
 * @param <VALUEOUT> the value output type from the Mapper
 */
public class MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
  extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  private RecordReader<KEYIN,VALUEIN> reader;
  private InputSplit split;

  public MapContext(Configuration conf, TaskAttemptID taskid,
                    RecordReader<KEYIN,VALUEIN> reader,
                    RecordWriter<KEYOUT,VALUEOUT> writer,
                    OutputCommitter committer,
                    StatusReporter reporter,
                    InputSplit split) {
    super(conf, taskid, writer, committer, reporter);
    this.reader = reader;
    this.split = split;
  }

  /**
   * Get the input split for this map.
   */
  public InputSplit getInputSplit() {
    return split;
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    return reader.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    return reader.getCurrentValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return reader.nextKeyValue();
  }

}
     