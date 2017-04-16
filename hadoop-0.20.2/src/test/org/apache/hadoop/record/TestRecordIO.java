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

package org.apache.hadoop.record;

import java.io.IOException;
import junit.framework.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 */
public class TestRecordIO extends TestCase {
    
  public TestRecordIO(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
    
  public void testBinary() {
    File tmpfile;
    try {
      tmpfile = File.createTempFile("hadooprec", ".dat");
      FileOutputStream ostream = new FileOutputStream(tmpfile);
      BinaryRecordOutput out = new BinaryRecordOutput(ostream);
      RecRecord1 r1 = new RecRecord1();
      r1.setBoolVal(true);
      r1.setByteVal((byte)0x66);
      r1.setFloatVal(3.145F);
      r1.setDoubleVal(1.5234);
      r1.setIntVal(-4567);
      r1.setLongVal(-2367L);
      r1.setStringVal("random text");
      r1.setBufferVal(new Buffer());
      r1.setVectorVal(new ArrayList<String>());
      r1.setMapVal(new TreeMap<String,String>());
      RecRecord0 r0 = new RecRecord0();
      r0.setStringVal("other random text");
      r1.setRecordVal(r0);
      r1.serialize(out, "");
      ostream.close();
      FileInputStream istream = new FileInputStream(tmpfile);
      BinaryRecordInput in = new BinaryRecordInput(istream);
      RecRecord1 r2 = new RecRecord1();
      r2.deserialize(in, "");
      istream.close();
      tmpfile.delete();
      assertTrue("Serialized and deserialized records do not match.", r1.equals(r2));
    } catch (IOException ex) {
      ex.printStackTrace();
    } 
  }
    
  public void testCsv() {
    File tmpfile;
    try {
      tmpfile = File.createTempFile("hadooprec", ".txt");
      FileOutputStream ostream = new FileOutputStream(tmpfile);
      CsvRecordOutput out = new CsvRecordOutput(ostream);
      RecRecord1 r1 = new RecRecord1();
      r1.setBoolVal(true);
      r1.setByteVal((byte)0x66);
      r1.setFloatVal(3.145F);
      r1.setDoubleVal(1.5234);
      r1.setIntVal(4567);
      r1.setLongVal(0x5a5a5a5a5a5aL);
      r1.setStringVal("random text");
      r1.setBufferVal(new Buffer());
      r1.setVectorVal(new ArrayList<String>());
      r1.setMapVal(new TreeMap<String,String>());
      RecRecord0 r0 = new RecRecord0();
      r0.setStringVal("other random text");
      r1.setRecordVal(r0);
      r1.serialize(out, "");
      ostream.close();
      FileInputStream istream = new FileInputStream(tmpfile);
      CsvRecordInput in = new CsvRecordInput(istream);
      RecRecord1 r2 = new RecRecord1();
      r2.deserialize(in, "");
      istream.close();
      tmpfile.delete();
      assertTrue("Serialized and deserialized records do not match.", r1.equals(r2));
            
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  public void testToString() {
    try {
      RecRecord1 r1 = new RecRecord1();
      r1.setBoolVal(true);
      r1.setByteVal((byte)0x66);
      r1.setFloatVal(3.145F);
      r1.setDoubleVal(1.5234);
      r1.setIntVal(4567);
      r1.setLongVal(0x5a5a5a5a5a5aL);
      r1.setStringVal("random text");
      byte[] barr = new byte[256];
      for (int idx = 0; idx < 256; idx++) {
        barr[idx] = (byte) idx;
      }
      r1.setBufferVal(new Buffer(barr));
      r1.setVectorVal(new ArrayList<String>());
      r1.setMapVal(new TreeMap<String,String>());
      RecRecord0 r0 = new RecRecord0();
      r0.setStringVal("other random text");
      r1.setRecordVal(r0);
      System.err.println("Illustrating toString bug"+r1.toString());
      System.err.println("Illustrating toString bug"+r1.toString());
    } catch (Throwable ex) {
      assertTrue("Record.toString cannot be invoked twice in succession."+
                 "This bug has been fixed in the latest version.", false);
    }
  }
    
  public void testXml() {
    File tmpfile;
    try {
      tmpfile = File.createTempFile("hadooprec", ".xml");
      FileOutputStream ostream = new FileOutputStream(tmpfile);
      XmlRecordOutput out = new XmlRecordOutput(ostream);
      RecRecord1 r1 = new RecRecord1();
      r1.setBoolVal(true);
      r1.setByteVal((byte)0x66);
      r1.setFloatVal(3.145F);
      r1.setDoubleVal(1.5234);
      r1.setIntVal(4567);
      r1.setLongVal(0x5a5a5a5a5a5aL);
      r1.setStringVal("ran\002dom &lt; %text<&more\uffff");
      r1.setBufferVal(new Buffer());
      r1.setVectorVal(new ArrayList<String>());
      r1.setMapVal(new TreeMap<String,String>());
      RecRecord0 r0 = new RecRecord0();
      r0.setStringVal("other %rando\007m &amp; >&more text");
      r1.setRecordVal(r0);
      r1.serialize(out, "");
      ostream.close();
      FileInputStream istream = new FileInputStream(tmpfile);
      XmlRecordInput in = new XmlRecordInput(istream);
      RecRecord1 r2 = new RecRecord1();
      r2.deserialize(in, "");
      istream.close();
      tmpfile.delete();
      assertTrue("Serialized and deserialized records do not match.", r1.equals(r2));
    } catch (IOException ex) {
      ex.printStackTrace();
    } 
  }
    
  public void testCloneable() {
    RecRecord1 r1 = new RecRecord1();
    r1.setBoolVal(true);
    r1.setByteVal((byte)0x66);
    r1.setFloatVal(3.145F);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(-4567);
    r1.setLongVal(-2367L);
    r1.setStringVal("random text");
    r1.setBufferVal(new Buffer());
    r1.setVectorVal(new ArrayList<String>());
    r1.setMapVal(new TreeMap<String,String>());
    RecRecord0 r0 = new RecRecord0();
    r0.setStringVal("other random text");
    r1.setRecordVal(r0);
    try {
      RecRecord1 r2 = (RecRecord1) r1.clone();
      assertTrue("Cloneable semantics violated. r1==r2", r1 != r2);
      assertTrue("Cloneable semantics violated. r1.getClass() != r2.getClass()",
                 r1.getClass() == r2.getClass());
      assertTrue("Cloneable semantics violated. !r2.equals(r1)", r2.equals(r1));
    } catch (final CloneNotSupportedException ex) {
      ex.printStackTrace();
    }
  }
}
