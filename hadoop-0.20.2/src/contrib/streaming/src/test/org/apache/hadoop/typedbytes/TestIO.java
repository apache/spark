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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.RecRecord0;
import org.apache.hadoop.record.RecRecord1;

import junit.framework.TestCase;

public class TestIO extends TestCase {

  private File tmpfile;

  protected void setUp() throws Exception {
    File testDir = new File(System.getProperty("test.build.data", "/tmp"));
    testDir.mkdir();
    this.tmpfile = new File(testDir, "typedbytes.bin");
  }

  protected void tearDown() throws Exception {
    tmpfile.delete();
  }

  public void testIO() throws IOException {
    ArrayList<Object> vector = new ArrayList<Object>();
    vector.add("test");
    vector.add(false);
    vector.add(12345);
    List<Object> list = new LinkedList<Object>();
    list.add("another test");
    list.add(true);
    list.add(123456789L);
    Map<Object, Object> map = new HashMap<Object, Object>();
    map.put("one", 1);
    map.put("vector", vector);
    Object[] objects = new Object[] {
      new Buffer(new byte[] { 1, 2, 3, 4 }),
      (byte) 123, true, 12345, 123456789L, (float) 1.2, 1.234,
      "random string", vector, list, map 
    };
    byte[] appSpecificBytes = new byte[] { 1, 2, 3 };

    FileOutputStream ostream = new FileOutputStream(tmpfile);
    DataOutputStream dostream = new DataOutputStream(ostream);
    TypedBytesOutput out = new TypedBytesOutput(dostream);
    for (Object obj : objects) {
      out.write(obj);
    }
    out.writeBytes(appSpecificBytes, 100);
    dostream.close();
    ostream.close();

    FileInputStream istream = new FileInputStream(tmpfile);
    DataInputStream distream = new DataInputStream(istream);
    TypedBytesInput in = new TypedBytesInput(distream);
    for (Object obj : objects) {
      assertEquals(obj, in.read());
    }
    assertEquals(new Buffer(appSpecificBytes), in.read());
    distream.close();
    istream.close();

    istream = new FileInputStream(tmpfile);
    distream = new DataInputStream(istream);
    in = new TypedBytesInput(distream);
    for (Object obj : objects) {
      byte[] bytes = in.readRaw();
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais);
      assertEquals(obj, (new TypedBytesInput(dis)).read());
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TypedBytesOutput tbout = new TypedBytesOutput(new DataOutputStream(baos));
      tbout.writeRaw(bytes);
      bais = new ByteArrayInputStream(bytes);
      dis = new DataInputStream(bais);
      assertEquals(obj, (new TypedBytesInput(dis)).read());
    }
    byte[] rawBytes = in.readRaw();
    assertEquals(new Buffer(appSpecificBytes),
      new Buffer(rawBytes, 5, rawBytes.length - 5));
    distream.close();
    istream.close();
  }

  public void testCustomTypesIO() throws IOException {
    byte[] rawBytes = new byte[] { 100, 0, 0, 0, 3, 1, 2, 3 };
    
    FileOutputStream ostream = new FileOutputStream(tmpfile);
    DataOutputStream dostream = new DataOutputStream(ostream);
    TypedBytesOutput out = new TypedBytesOutput(dostream);
    out.writeRaw(rawBytes);
    dostream.close();
    ostream.close();

    FileInputStream istream = new FileInputStream(tmpfile);
    DataInputStream distream = new DataInputStream(istream);
    TypedBytesInput in = new TypedBytesInput(distream);
    assertTrue(Arrays.equals(rawBytes, in.readRaw()));
    distream.close();
    istream.close();
  }
  
  public void testRecordIO() throws IOException {
    RecRecord1 r1 = new RecRecord1();
    r1.setBoolVal(true);
    r1.setByteVal((byte) 0x66);
    r1.setFloatVal(3.145F);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(-4567);
    r1.setLongVal(-2367L);
    r1.setStringVal("random text");
    r1.setBufferVal(new Buffer());
    r1.setVectorVal(new ArrayList<String>());
    r1.setMapVal(new TreeMap<String, String>());
    RecRecord0 r0 = new RecRecord0();
    r0.setStringVal("other random text");
    r1.setRecordVal(r0);

    FileOutputStream ostream = new FileOutputStream(tmpfile);
    DataOutputStream dostream = new DataOutputStream(ostream);
    TypedBytesRecordOutput out = TypedBytesRecordOutput.get(dostream);
    r1.serialize(out, "");
    dostream.close();
    ostream.close();

    FileInputStream istream = new FileInputStream(tmpfile);
    DataInputStream distream = new DataInputStream(istream);
    TypedBytesRecordInput in = TypedBytesRecordInput.get(distream);
    RecRecord1 r2 = new RecRecord1();
    r2.deserialize(in, "");
    distream.close();
    istream.close();
    assertEquals(r1, r2);
  }

  public void testWritableIO() throws IOException {
    Writable[] vectorValues = new Writable[] {
      new Text("test1"), new Text("test2"), new Text("test3")
    };
    ArrayWritable vector = new ArrayWritable(Text.class, vectorValues);
    MapWritable map = new MapWritable();
    map.put(new Text("one"), new VIntWritable(1));
    map.put(new Text("two"), new VLongWritable(2));
    Writable[] writables = new Writable[] {
      new BytesWritable(new byte[] { 1, 2, 3, 4 }),
      new ByteWritable((byte) 123), new BooleanWritable(true),
      new VIntWritable(12345), new VLongWritable(123456789L),
      new FloatWritable((float) 1.2), new DoubleWritable(1.234),
      new Text("random string"),
      new ObjectWritable("test")
    };
    TypedBytesWritable tbw = new TypedBytesWritable();
    tbw.setValue("typed bytes text");
    RecRecord1 r1 = new RecRecord1();
    r1.setBoolVal(true);
    r1.setByteVal((byte) 0x66);
    r1.setFloatVal(3.145F);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(-4567);
    r1.setLongVal(-2367L);
    r1.setStringVal("random text");
    r1.setBufferVal(new Buffer());
    r1.setVectorVal(new ArrayList<String>());
    r1.setMapVal(new TreeMap<String, String>());
    RecRecord0 r0 = new RecRecord0();
    r0.setStringVal("other random text");
    r1.setRecordVal(r0);

    FileOutputStream ostream = new FileOutputStream(tmpfile);
    DataOutputStream dostream = new DataOutputStream(ostream);
    TypedBytesWritableOutput out = new TypedBytesWritableOutput(dostream);
    for (Writable w : writables) {
      out.write(w);
    }
    out.write(tbw);
    out.write(vector);
    out.write(map);
    out.write(r1);
    dostream.close();
    ostream.close();

    FileInputStream istream = new FileInputStream(tmpfile);
    DataInputStream distream = new DataInputStream(istream);

    TypedBytesWritableInput in = new TypedBytesWritableInput(distream);
    for (Writable w : writables) {
      assertEquals(w.toString(), in.read().toString());
    }

    assertEquals(tbw.getValue().toString(), in.read().toString());

    assertEquals(ArrayWritable.class, in.readType());
    ArrayWritable aw = in.readArray();
    Writable[] writables1 = vector.get(), writables2 = aw.get();
    assertEquals(writables1.length, writables2.length);
    for (int i = 0; i < writables1.length; i++) {
      assertEquals(((Text) writables1[i]).toString(),
        ((TypedBytesWritable) writables2[i]).getValue());
    }
    assertEquals(MapWritable.class, in.readType());

    MapWritable mw = in.readMap();
    assertEquals(map.entrySet(), mw.entrySet());

    assertEquals(Type.LIST, TypedBytesInput.get(distream).readType());
    assertEquals(r1.getBoolVal(), TypedBytesInput.get(distream).read());
    assertEquals(r1.getByteVal(), TypedBytesInput.get(distream).read());
    assertEquals(r1.getIntVal(), TypedBytesInput.get(distream).read());
    assertEquals(r1.getLongVal(), TypedBytesInput.get(distream).read());
    assertEquals(r1.getFloatVal(), TypedBytesInput.get(distream).read());
    assertEquals(r1.getDoubleVal(), TypedBytesInput.get(distream).read());
    assertEquals(r1.getStringVal(), TypedBytesInput.get(distream).read());
    Object prevObj = null, obj = TypedBytesInput.get(distream).read();
    while (obj != null) {
      prevObj = obj;
      obj = TypedBytesInput.get(distream).read();
    }
    List recList = (List) prevObj;
    assertEquals(r0.getStringVal(), recList.get(0));

    distream.close();
    istream.close();
  }

}
