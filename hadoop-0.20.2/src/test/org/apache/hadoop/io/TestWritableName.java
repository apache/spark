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

package org.apache.hadoop.io;

import java.io.*;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import junit.framework.TestCase;

/** Unit tests for WritableName. */
public class TestWritableName extends TestCase {
  public TestWritableName(String name) { 
    super(name); 
  }

  /** Example class used in test cases below. */
  public static class SimpleWritable implements Writable {
    private static final Random RANDOM = new Random();

    int state = RANDOM.nextInt();

    public void write(DataOutput out) throws IOException {
      out.writeInt(state);
    }

    public void readFields(DataInput in) throws IOException {
      this.state = in.readInt();
    }

    public static SimpleWritable read(DataInput in) throws IOException {
      SimpleWritable result = new SimpleWritable();
      result.readFields(in);
      return result;
    }

    /** Required by test code, below. */
    public boolean equals(Object o) {
      if (!(o instanceof SimpleWritable))
        return false;
      SimpleWritable other = (SimpleWritable)o;
      return this.state == other.state;
    }
  }

  private static final String testName = "mystring";

  public void testGoodName() throws Exception {
    Configuration conf = new Configuration();
    Class<?> test = WritableName.getClass("long",conf);
    assertTrue(test != null);
  }

  public void testSetName() throws Exception {
    Configuration conf = new Configuration();
    WritableName.setName(SimpleWritable.class, testName);

    Class<?> test = WritableName.getClass(testName,conf);
    assertTrue(test.equals(SimpleWritable.class));
  }


  public void testAddName() throws Exception {
    Configuration conf = new Configuration();
    String altName = testName + ".alt";

    WritableName.addName(SimpleWritable.class, altName);

    Class<?> test = WritableName.getClass(altName, conf);
    assertTrue(test.equals(SimpleWritable.class));

    // check original name still works
    test = WritableName.getClass(testName, conf);
    assertTrue(test.equals(SimpleWritable.class));

  }

  public void testBadName() throws Exception {
    Configuration conf = new Configuration();
    try {
      Class<?> test = WritableName.getClass("unknown_junk",conf);
      assertTrue(false);
    } catch(IOException e) {
      assertTrue(e.getMessage().matches(".*unknown_junk.*"));
    }
  }
	
}
