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
package org.apache.hadoop.mrunit.mock;

import junit.framework.TestCase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mrunit.MapDriver;

import org.junit.Test;


public class TestMockOutputCollector extends TestCase {

  /**
   * A mapper that reuses the same key and val objects to emit multiple values
   */
  class RepeatMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
    public void map(Text k, Text v, OutputCollector<Text, Text> out, Reporter r)
        throws IOException {
      Text outKey = new Text();
      Text outVal = new Text();

      outKey.set("1");
      outVal.set("a");
      out.collect(outKey, outVal);

      outKey.set("2");
      outVal.set("b");
      out.collect(outKey, outVal);

      outKey.set("3");
      outVal.set("c");
      out.collect(outKey, outVal);
    }
  }

  @Test
  public void testRepeatedObjectUse() {
    Mapper<Text, Text, Text, Text> mapper = new RepeatMapper();
    MapDriver<Text, Text, Text, Text> driver = new MapDriver(mapper);

    driver.withInput(new Text("inK"), new Text("inV"))
          .withOutput(new Text("1"), new Text("a"))
          .withOutput(new Text("2"), new Text("b"))
          .withOutput(new Text("3"), new Text("c"))
          .runTest();
  }
}

