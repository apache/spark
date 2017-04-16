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
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 */
public class ToCpp extends TestCase {
    
  public ToCpp(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
    
  public void testBinary() {
    File tmpfile;
    try {
      tmpfile = new File("/tmp/hadooptemp.dat");
      FileOutputStream ostream = new FileOutputStream(tmpfile);
      BinaryRecordOutput out = new BinaryRecordOutput(ostream);
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
      r1.serialize(out, "");
      ostream.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    } 
  }
    
  public void testCsv() {
    File tmpfile;
    try {
      tmpfile = new File("/tmp/hadooptemp.txt");
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
      r1.serialize(out, "");
      ostream.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    } 
  }

  public void testXml() {
    File tmpfile;
    try {
      tmpfile = new File("/tmp/hadooptemp.xml");
      FileOutputStream ostream = new FileOutputStream(tmpfile);
      XmlRecordOutput out = new XmlRecordOutput(ostream);
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
      r1.serialize(out, "");
      ostream.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    } 
  }
}
