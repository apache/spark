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
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * @see TestDelegatingInputFormat
 */
public class TestMultipleInputs extends TestCase {
  
  public void testAddInputPathWithFormat() {
    final JobConf conf = new JobConf();
    MultipleInputs.addInputPath(conf, new Path("/foo"), TextInputFormat.class);
    MultipleInputs.addInputPath(conf, new Path("/bar"),
        KeyValueTextInputFormat.class);
    final Map<Path, InputFormat> inputs = MultipleInputs
       .getInputFormatMap(conf);
    assertEquals(TextInputFormat.class, inputs.get(new Path("/foo")).getClass());
    assertEquals(KeyValueTextInputFormat.class, inputs.get(new Path("/bar"))
       .getClass());
  }

  public void testAddInputPathWithMapper() {
    final JobConf conf = new JobConf();
    MultipleInputs.addInputPath(conf, new Path("/foo"), TextInputFormat.class,
       MapClass.class);
    MultipleInputs.addInputPath(conf, new Path("/bar"),
       KeyValueTextInputFormat.class, MapClass2.class);
    final Map<Path, InputFormat> inputs = MultipleInputs
       .getInputFormatMap(conf);
    final Map<Path, Class<? extends Mapper>> maps = MultipleInputs
       .getMapperTypeMap(conf);

    assertEquals(TextInputFormat.class, inputs.get(new Path("/foo")).getClass());
    assertEquals(KeyValueTextInputFormat.class, inputs.get(new Path("/bar"))
       .getClass());
    assertEquals(MapClass.class, maps.get(new Path("/foo")));
    assertEquals(MapClass2.class, maps.get(new Path("/bar")));
  }

  static class MapClass implements Mapper<String, String, String, String> {

    public void map(String key, String value,
       OutputCollector<String, String> output, Reporter reporter)
       throws IOException {
    }

    public void configure(JobConf job) {
    }

    public void close() throws IOException {
    }
  }

  static class MapClass2 extends MapClass {
  }
}
