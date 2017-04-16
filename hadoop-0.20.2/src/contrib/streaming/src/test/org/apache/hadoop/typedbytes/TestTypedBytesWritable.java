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

package org.apache.hadoop.typedbytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class TestTypedBytesWritable extends TestCase {

  public void testToString() {
    TypedBytesWritable tbw = new TypedBytesWritable();
    tbw.setValue(true);
    assertEquals("true", tbw.toString());
    tbw.setValue(12345);
    assertEquals("12345", tbw.toString());
    tbw.setValue(123456789L);
    assertEquals("123456789", tbw.toString());
    tbw.setValue((float) 1.23);
    assertEquals("1.23", tbw.toString());
    tbw.setValue(1.23456789);
    assertEquals("1.23456789", tbw.toString());
    tbw.setValue("random text");
    assertEquals("random text", tbw.toString());
  }

  public void testIO() throws IOException {
    TypedBytesWritable tbw = new TypedBytesWritable();
    tbw.setValue(12345);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput dout = new DataOutputStream(baos);
    tbw.write(dout);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput din = new DataInputStream(bais);
    TypedBytesWritable readTbw = new TypedBytesWritable();
    readTbw.readFields(din);
    assertEquals(tbw, readTbw);
  }

}
