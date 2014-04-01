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

package testjar;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExternalMapperReducer
  implements Mapper<WritableComparable, Writable,
                    ExternalWritable, IntWritable>,
             Reducer<WritableComparable, Writable,
                     WritableComparable, IntWritable> {

  public void configure(JobConf job) {

  }

  public void close()
    throws IOException {

  }

  public void map(WritableComparable key, Writable value,
                  OutputCollector<ExternalWritable, IntWritable> output,
                  Reporter reporter)
    throws IOException {
    
    if (value instanceof Text) {
      Text text = (Text)value;
      ExternalWritable ext = new ExternalWritable(text.toString());
      output.collect(ext, new IntWritable(1));
    }
  }

  public void reduce(WritableComparable key, Iterator<Writable> values,
                     OutputCollector<WritableComparable, IntWritable> output,
                     Reporter reporter)
    throws IOException {
    
    int count = 0;
    while (values.hasNext()) {
      count++;
      values.next();
    }
    output.collect(key, new IntWritable(count));
  }
}
