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

import junit.framework.TestCase;
import java.util.Random;

/** Unit tests for UTF8. */
@SuppressWarnings("deprecation")
public class TestUTF8 extends TestCase {
  public TestUTF8(String name) { super(name); }

  private static final Random RANDOM = new Random();

  public static String getTestString() throws Exception {
    StringBuffer buffer = new StringBuffer();
    int length = RANDOM.nextInt(100);
    for (int i = 0; i < length; i++) {
      buffer.append((char)(RANDOM.nextInt(Character.MAX_VALUE)));
    }
    return buffer.toString();
  }

  public void testWritable() throws Exception {
    for (int i = 0; i < 10000; i++) {
      TestWritable.testWritable(new UTF8(getTestString()));
    }
  }

  public void testGetBytes() throws Exception {
    for (int i = 0; i < 10000; i++) {

      // generate a random string
      String before = getTestString();

      // check its utf8
      assertEquals(before, new String(UTF8.getBytes(before), "UTF-8"));
    }
  }

  public void testIO() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    for (int i = 0; i < 10000; i++) {
      // generate a random string
      String before = getTestString();

      // write it
      out.reset();
      UTF8.writeString(out, before);

      // test that it reads correctly
      in.reset(out.getData(), out.getLength());
      String after = UTF8.readString(in);
      assertTrue(before.equals(after));

      // test that it reads correctly with DataInput
      in.reset(out.getData(), out.getLength());
      String after2 = in.readUTF();
      assertTrue(before.equals(after2));

      // test that it is compatible with Java's other decoder
      String after3 = new String(out.getData(), 2, out.getLength()-2, "UTF-8");
      assertTrue(before.equals(after3));

    }

  }

  public void testNullEncoding() throws Exception {
    String s = new String(new char[] { 0 });

    DataOutputBuffer dob = new DataOutputBuffer();
    new UTF8(s).write(dob);

    assertEquals(s, new String(dob.getData(), 2, dob.getLength()-2, "UTF-8"));
  }
	
}
