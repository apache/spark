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

import java.io.DataInput;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.InputStream;

/**
 */
public class BinaryRecordInput implements RecordInput {
    
  private DataInput in;
    
  static private class BinaryIndex implements Index {
    private int nelems;
    private BinaryIndex(int nelems) {
      this.nelems = nelems;
    }
    public boolean done() {
      return (nelems <= 0);
    }
    public void incr() {
      nelems--;
    }
  }
    
  private BinaryRecordInput() {}
    
  private void setDataInput(DataInput inp) {
    this.in = inp;
  }
    
  private static ThreadLocal bIn = new ThreadLocal() {
      protected synchronized Object initialValue() {
        return new BinaryRecordInput();
      }
    };
    
  /**
   * Get a thread-local record input for the supplied DataInput.
   * @param inp data input stream
   * @return binary record input corresponding to the supplied DataInput.
   */
  public static BinaryRecordInput get(DataInput inp) {
    BinaryRecordInput bin = (BinaryRecordInput) bIn.get();
    bin.setDataInput(inp);
    return bin;
  }
    
  /** Creates a new instance of BinaryRecordInput */
  public BinaryRecordInput(InputStream strm) {
    this.in = new DataInputStream(strm);
  }
    
  /** Creates a new instance of BinaryRecordInput */
  public BinaryRecordInput(DataInput din) {
    this.in = din;
  }
    
  public byte readByte(final String tag) throws IOException {
    return in.readByte();
  }
    
  public boolean readBool(final String tag) throws IOException {
    return in.readBoolean();
  }
    
  public int readInt(final String tag) throws IOException {
    return Utils.readVInt(in);
  }
    
  public long readLong(final String tag) throws IOException {
    return Utils.readVLong(in);
  }
    
  public float readFloat(final String tag) throws IOException {
    return in.readFloat();
  }
    
  public double readDouble(final String tag) throws IOException {
    return in.readDouble();
  }
    
  public String readString(final String tag) throws IOException {
    return Utils.fromBinaryString(in);
  }
    
  public Buffer readBuffer(final String tag) throws IOException {
    final int len = Utils.readVInt(in);
    final byte[] barr = new byte[len];
    in.readFully(barr);
    return new Buffer(barr);
  }
    
  public void startRecord(final String tag) throws IOException {
    // no-op
  }
    
  public void endRecord(final String tag) throws IOException {
    // no-op
  }
    
  public Index startVector(final String tag) throws IOException {
    return new BinaryIndex(readInt(tag));
  }
    
  public void endVector(final String tag) throws IOException {
    // no-op
  }
    
  public Index startMap(final String tag) throws IOException {
    return new BinaryIndex(readInt(tag));
  }
    
  public void endMap(final String tag) throws IOException {
    // no-op
  }
}
