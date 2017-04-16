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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An {@link Mapper} that delegates behaviour of paths to multiple other
 * mappers.
 * 
 * @see MultipleInputs#addInputPath(JobConf, Path, Class, Class)
 */
public class DelegatingMapper<K1, V1, K2, V2> implements Mapper<K1, V1, K2, V2> {

  private JobConf conf;

  private Mapper<K1, V1, K2, V2> mapper;

  @SuppressWarnings("unchecked")
  public void map(K1 key, V1 value, OutputCollector<K2, V2> outputCollector,
      Reporter reporter) throws IOException {

    if (mapper == null) {
      // Find the Mapper from the TaggedInputSplit.
      TaggedInputSplit inputSplit = (TaggedInputSplit) reporter.getInputSplit();
      mapper = (Mapper<K1, V1, K2, V2>) ReflectionUtils.newInstance(inputSplit
         .getMapperClass(), conf);
    }
    mapper.map(key, value, outputCollector, reporter);
  }

  public void configure(JobConf conf) {
    this.conf = conf;
  }

  public void close() throws IOException {
    if (mapper != null) {
      mapper.close();
    }
  }

}
