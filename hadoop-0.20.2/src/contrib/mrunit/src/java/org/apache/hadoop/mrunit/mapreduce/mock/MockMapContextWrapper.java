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

package org.apache.hadoop.mrunit.mapreduce.mock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.mock.MockOutputCollector;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * o.a.h.mapreduce.Mapper.map() expects to use a Mapper.Context
 * object as a parameter. We want to override the functionality
 * of a lot of Context to have it send the results back to us, etc.
 * But since Mapper.Context is an inner class of Mapper, we need to
 * put any subclasses of Mapper.Context in a subclass of Mapper.
 *
 * This wrapper class exists for that purpose.
 */
public class MockMapContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  public static final Log LOG = LogFactory.getLog(MockMapContextWrapper.class);

  /**
   * Mock context instance that provides input to and receives output from
   * the Mapper instance under test.
   */
  public class MockMapContext extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

    private Iterator<Pair<KEYIN, VALUEIN>> inputIter;
    private Pair<KEYIN, VALUEIN> curInput;
    private MockOutputCollector<KEYOUT, VALUEOUT> output;

    /**
     * Create a new instance with the passed configuration, map key/value input 
     * pairs and counters
     * 
     * @param configuration Configuration for the mapper
     * @param in input key/value pairs for the mapper
     * @param counters pre-initialized counter values
     */
    public MockMapContext(final Configuration configuration,
        final List<Pair<KEYIN, VALUEIN>> in,
        final Counters counters) throws IOException, InterruptedException {

      super(configuration,
            new TaskAttemptID("mrunit-jt", 0, true, 0, 0),
            null, null, new MockOutputCommitter(), new MockReporter(counters), null);
      this.inputIter = in.iterator();
      this.output = new MockOutputCollector<KEYOUT, VALUEOUT>();
    }

    /**
     * Create a new instance with the passed map key/value input pairs and
     * counters. A new {@link Configuration} object will be created and used
     * to configure the mapper
     * 
     * @param in input key/value pairs for the mapper
     * @param counters pre-initialized counter values
     */
    public MockMapContext(final List<Pair<KEYIN, VALUEIN>> in,
        final Counters counters)
        throws IOException, InterruptedException {
      this(new Configuration(), in, counters);
    }


    @Override
    public InputSplit getInputSplit() {
      return new MockInputSplit();
    }

    @Override
    public KEYIN getCurrentKey() {
      return curInput.getFirst();
    }

    @Override
    public VALUEIN getCurrentValue() {
      return curInput.getSecond();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      if (this.inputIter.hasNext()) {
        this.curInput = this.inputIter.next();
        return true;
      } else {
        return false;
      }
    }

    public void write(KEYOUT key, VALUEOUT value) throws IOException {
      output.collect(key, value);
    }

    @Override
    /** This method does nothing in the mock version. */
    public void progress() {
    }

    @Override
    /** This method does nothing in the mock version. */
    public void setStatus(String status) {
    }

    /**
     * @return the outputs from the MockOutputCollector back to
     * the test harness.
     */
    public List<Pair<KEYOUT, VALUEOUT>> getOutputs() {
      return output.getOutputs();
    }
  }

  public MockMapContext getMockContext(Configuration configuration,
      List<Pair<KEYIN, VALUEIN>> inputs, Counters counters)
      throws IOException, InterruptedException {
    return new MockMapContext(configuration, inputs, counters);
  }

  public MockMapContext getMockContext(
      List<Pair<KEYIN, VALUEIN>> inputs, Counters counters)
      throws IOException, InterruptedException {
    return new MockMapContext(inputs, counters);
  }
}

