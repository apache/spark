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
import java.util.TreeMap;
import java.util.ArrayList;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 */
public class BinaryRecordOutput implements RecordOutput {
    
  private DataOutput out;
    
  private BinaryRecordOutput() {}
    
  private void setDataOutput(DataOutput out) {
    this.out = out;
  }
    
  private static ThreadLocal bOut = new ThreadLocal() {
      protected synchronized Object initialValue() {
        return new BinaryRecordOutput();
      }
    };
    
  /**
   * Get a thread-local record output for the supplied DataOutput.
   * @param out data output stream
   * @return binary record output corresponding to the supplied DataOutput.
   */
  public static BinaryRecordOutput get(DataOutput out) {
    BinaryRecordOutput bout = (BinaryRecordOutput) bOut.get();
    bout.setDataOutput(out);
    return bout;
  }
    
  /** Creates a new instance of BinaryRecordOutput */
  public BinaryRecordOutput(OutputStream out) {
    this.out = new DataOutputStream(out);
  }
    
  /** Creates a new instance of BinaryRecordOutput */
  public BinaryRecordOutput(DataOutput out) {
    this.out = out;
  }
    
    
  public void writeByte(byte b, String tag) throws IOException {
    out.writeByte(b);
  }
    
  public void writeBool(boolean b, String tag) throws IOException {
    out.writeBoolean(b);
  }
    
  public void writeInt(int i, String tag) throws IOException {
    Utils.writeVInt(out, i);
  }
    
  public void writeLong(long l, String tag) throws IOException {
    Utils.writeVLong(out, l);
  }
    
  public void writeFloat(float f, String tag) throws IOException {
    out.writeFloat(f);
  }
    
  public void writeDouble(double d, String tag) throws IOException {
    out.writeDouble(d);
  }
    
  public void writeString(String s, String tag) throws IOException {
    Utils.toBinaryString(out, s);
  }
    
  public void writeBuffer(Buffer buf, String tag)
    throws IOException {
    byte[] barr = buf.get();
    int len = buf.getCount();
    Utils.writeVInt(out, len);
    out.write(barr, 0, len);
  }
    
  public void startRecord(Record r, String tag) throws IOException {}
    
  public void endRecord(Record r, String tag) throws IOException {}
    
  public void startVector(ArrayList v, String tag) throws IOException {
    writeInt(v.size(), tag);
  }
    
  public void endVector(ArrayList v, String tag) throws IOException {}
    
  public void startMap(TreeMap v, String tag) throws IOException {
    writeInt(v.size(), tag);
  }
    
  public void endMap(TreeMap v, String tag) throws IOException {}
    
}
