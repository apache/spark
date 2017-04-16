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
package org.apache.hadoop.mapred.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TestTupleWritable extends TestCase {

  private TupleWritable makeTuple(Writable[] writs) {
    Writable[] sub1 = { writs[1], writs[2] };
    Writable[] sub3 = { writs[4], writs[5] };
    Writable[] sub2 = { writs[3], new TupleWritable(sub3), writs[6] };
    Writable[] vals = { writs[0], new TupleWritable(sub1),
                        new TupleWritable(sub2), writs[7], writs[8],
                        writs[9] };
    // [v0, [v1, v2], [v3, [v4, v5], v6], v7, v8, v9]
    TupleWritable ret = new TupleWritable(vals);
    for (int i = 0; i < 6; ++i) {
      ret.setWritten(i);
    }
    ((TupleWritable)sub2[1]).setWritten(0);
    ((TupleWritable)sub2[1]).setWritten(1);
    ((TupleWritable)vals[1]).setWritten(0);
    ((TupleWritable)vals[1]).setWritten(1);
    for (int i = 0; i < 3; ++i) {
      ((TupleWritable)vals[2]).setWritten(i);
    }
    return ret;
  }

  private int verifIter(Writable[] writs, TupleWritable t, int i) {
    for (Writable w : t) {
      if (w instanceof TupleWritable) {
        i = verifIter(writs, ((TupleWritable)w), i);
        continue;
      }
      assertTrue("Bad value", w.equals(writs[i++]));
    }
    return i;
  }

  public void testIterable() throws Exception {
    Random r = new Random();
    Writable[] writs = {
      new BooleanWritable(r.nextBoolean()),
      new FloatWritable(r.nextFloat()),
      new FloatWritable(r.nextFloat()),
      new IntWritable(r.nextInt()),
      new LongWritable(r.nextLong()),
      new BytesWritable("dingo".getBytes()),
      new LongWritable(r.nextLong()),
      new IntWritable(r.nextInt()),
      new BytesWritable("yak".getBytes()),
      new IntWritable(r.nextInt())
    };
    TupleWritable t = new TupleWritable(writs);
    for (int i = 0; i < 6; ++i) {
      t.setWritten(i);
    }
    verifIter(writs, t, 0);
  }

  public void testNestedIterable() throws Exception {
    Random r = new Random();
    Writable[] writs = {
      new BooleanWritable(r.nextBoolean()),
      new FloatWritable(r.nextFloat()),
      new FloatWritable(r.nextFloat()),
      new IntWritable(r.nextInt()),
      new LongWritable(r.nextLong()),
      new BytesWritable("dingo".getBytes()),
      new LongWritable(r.nextLong()),
      new IntWritable(r.nextInt()),
      new BytesWritable("yak".getBytes()),
      new IntWritable(r.nextInt())
    };
    TupleWritable sTuple = makeTuple(writs);
    assertTrue("Bad count", writs.length == verifIter(writs, sTuple, 0));
  }

  public void testWritable() throws Exception {
    Random r = new Random();
    Writable[] writs = {
      new BooleanWritable(r.nextBoolean()),
      new FloatWritable(r.nextFloat()),
      new FloatWritable(r.nextFloat()),
      new IntWritable(r.nextInt()),
      new LongWritable(r.nextLong()),
      new BytesWritable("dingo".getBytes()),
      new LongWritable(r.nextLong()),
      new IntWritable(r.nextInt()),
      new BytesWritable("yak".getBytes()),
      new IntWritable(r.nextInt())
    };
    TupleWritable sTuple = makeTuple(writs);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    sTuple.write(new DataOutputStream(out));
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    TupleWritable dTuple = new TupleWritable();
    dTuple.readFields(new DataInputStream(in));
    assertTrue("Failed to write/read tuple", sTuple.equals(dTuple));
  }

  public void testWideTuple() throws Exception {
    Text emptyText = new Text("Should be empty");
    Writable[] values = new Writable[64];
    Arrays.fill(values,emptyText);
    values[42] = new Text("Number 42");
                                     
    TupleWritable tuple = new TupleWritable(values);
    tuple.setWritten(42);
    
    for (int pos=0; pos<tuple.size();pos++) {
      boolean has = tuple.has(pos);
      if (pos == 42) {
        assertTrue(has);
      }
      else {
        assertFalse("Tuple position is incorrectly labelled as set: " + pos, has);
      }
    }
  }
  
  public void testWideTuple2() throws Exception {
    Text emptyText = new Text("Should be empty");
    Writable[] values = new Writable[64];
    Arrays.fill(values,emptyText);
    values[9] = new Text("Number 9");
                                     
    TupleWritable tuple = new TupleWritable(values);
    tuple.setWritten(9);
    
    for (int pos=0; pos<tuple.size();pos++) {
      boolean has = tuple.has(pos);
      if (pos == 9) {
        assertTrue(has);
      }
      else {
        assertFalse("Tuple position is incorrectly labelled as set: " + pos, has);
      }
    }
  }
}
