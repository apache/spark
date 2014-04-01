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
package org.apache.hadoop.mrunit;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class TestPipelineMapReduceDriver extends TestCase {

  private static final int FOO_IN_A = 42;
  private static final int FOO_IN_B = 10;
  private static final int BAR_IN   = 12;
  private static final int FOO_OUT  = 52;
  private static final int BAR_OUT  = 12;

  @Test
  public void testFullyEmpty() throws IOException {
    // If no mappers or reducers are configured, then it should
    // just return its inputs. If there are no inputs, this
    // should be an empty list of outputs.
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    List out = driver.run();
    assertEquals("Expected empty output list", out.size(), 0);
  }

  @Test
  public void testEmptyPipeline() throws IOException {
    // If no mappers or reducers are configured, then it should
    // just return its inputs.
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    driver.addInput(new Text("foo"), new Text("bar"));
    List out = driver.run();

    List expected = new ArrayList();
    expected.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    assertListEquals(expected, out);
  }

  @Test
  public void testEmptyPipelineWithRunTest() {
    // Like testEmptyPipeline, but call runTest.
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    driver.withInput(new Text("foo"), new Text("bar"))
          .withOutput(new Text("foo"), new Text("bar"))
          .runTest();
  }


  @Test
  public void testSingleIdentity() {
    // Test that an identity mapper and identity reducer work
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    driver.withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withInput(new Text("foo"), new Text("bar"))
          .withOutput(new Text("foo"), new Text("bar"))
          .runTest();
  }

  @Test
  public void testMultipleIdentities() {
    // Test that a pipeline of identity mapper and reducers work
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    driver.withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withInput(new Text("foo"), new Text("bar"))
          .withOutput(new Text("foo"), new Text("bar"))
          .runTest();
  }

  @Test
  public void testSumAtEnd() {
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    driver.withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withMapReduce(new IdentityMapper(), new LongSumReducer())
          .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
          .withInput(new Text("bar"), new LongWritable(BAR_IN))
          .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
          .withOutput(new Text("bar"), new LongWritable(BAR_OUT))
          .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
          .runTest();
  }

  @Test
  public void testSumInMiddle() {
    PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
    driver.withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withMapReduce(new IdentityMapper(), new LongSumReducer())
          .withMapReduce(new IdentityMapper(), new IdentityReducer())
          .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
          .withInput(new Text("bar"), new LongWritable(BAR_IN))
          .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
          .withOutput(new Text("bar"), new LongWritable(BAR_OUT))
          .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
          .runTest();
  }
}

