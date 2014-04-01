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

/** Unit tests for Writable. */
public class TestWritable extends TestCase {
  public TestWritable(String name) { super(name); }

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

  /** Test 1: Check that SimpleWritable. */
  public void testSimpleWritable() throws Exception {
    testWritable(new SimpleWritable());
  }
  
  public void testByteWritable() throws Exception {
    testWritable(new ByteWritable((byte)128));
  }

  public void testDoubleWritable() throws Exception {
    testWritable(new DoubleWritable(1.0));
  }

  /** Utility method for testing writables. */
  public static Writable testWritable(Writable before) 
  	throws Exception {
  	return testWritable(before, null);
  }
  
  /** Utility method for testing writables. */
  public static Writable testWritable(Writable before
  		, Configuration conf) throws Exception {
    DataOutputBuffer dob = new DataOutputBuffer();
    before.write(dob);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), dob.getLength());
    
    Writable after = (Writable)ReflectionUtils.newInstance(
    		before.getClass(), conf);
    after.readFields(dib);

    assertEquals(before, after);
    return after;
  }
	
  private static class FrobComparator extends WritableComparator {
    public FrobComparator() { super(Frob.class); }
    @Override public int compare(byte[] b1, int s1, int l1,
                                 byte[] b2, int s2, int l2) {
      return 0;
    }
  }

  private static class Frob implements WritableComparable {
    static {                                     // register default comparator
      WritableComparator.define(Frob.class, new FrobComparator());
    }
    @Override public void write(DataOutput out) throws IOException {}
    @Override public void readFields(DataInput in) throws IOException {}
    @Override public int compareTo(Object o) { return 0; }
  }

  /** Test that comparator is defined. */
  public static void testGetComparator() throws Exception {
    assert(WritableComparator.get(Frob.class) instanceof FrobComparator);
  }

  private static class Foo implements WritableComparable {
    @Override public void write(DataOutput out) throws IOException {}
    @Override public void readFields(DataInput in) throws IOException {}
    @Override public int compareTo(Object o) { return 0; }
    // No comparator registered
  }
  
  public static void testUnregisteredComparators() throws Exception {
    WritableComparator firstComparator = WritableComparator.get(Foo.class);
    WritableComparator secondComparator = WritableComparator.get(Foo.class);
    assertNotSame("Should create a new comparator instance for unregistered " +
        "comparators", firstComparator, secondComparator);
  }
}
