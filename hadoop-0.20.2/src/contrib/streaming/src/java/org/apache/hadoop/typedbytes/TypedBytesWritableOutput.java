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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Record;

/**
 * Provides functionality for writing Writable objects as typed bytes.
 * 
 * @see TypedBytesOutput
 */
public class TypedBytesWritableOutput {

  private TypedBytesOutput out;

  private TypedBytesWritableOutput() {}

  private void setTypedBytesOutput(TypedBytesOutput out) {
    this.out = out;
  }

  private static ThreadLocal tbOut = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return new TypedBytesWritableOutput();
    }
  };

  /**
   * Get a thread-local typed bytes writable input for the supplied
   * {@link TypedBytesOutput}.
   * 
   * @param out typed bytes output object
   * @return typed bytes writable output corresponding to the supplied
   *         {@link TypedBytesOutput}.
   */
  public static TypedBytesWritableOutput get(TypedBytesOutput out) {
    TypedBytesWritableOutput bout = (TypedBytesWritableOutput) tbOut.get();
    bout.setTypedBytesOutput(out);
    return bout;
  }

  /**
   * Get a thread-local typed bytes writable output for the supplied
   * {@link DataOutput}.
   * 
   * @param out data output object
   * @return typed bytes writable output corresponding to the supplied
   *         {@link DataOutput}.
   */
  public static TypedBytesWritableOutput get(DataOutput out) {
    return get(TypedBytesOutput.get(out));
  }

  /** Creates a new instance of TypedBytesWritableOutput. */
  public TypedBytesWritableOutput(TypedBytesOutput out) {
    this();
    this.out = out;
  }

  /** Creates a new instance of TypedBytesWritableOutput. */
  public TypedBytesWritableOutput(DataOutput dout) {
    this(new TypedBytesOutput(dout));
  }

  public void write(Writable w) throws IOException {
    if (w instanceof TypedBytesWritable) {
      writeTypedBytes((TypedBytesWritable) w);
    } else if (w instanceof BytesWritable) {
      writeBytes((BytesWritable) w);
    } else if (w instanceof ByteWritable) {
      writeByte((ByteWritable) w);
    } else if (w instanceof BooleanWritable) {
      writeBoolean((BooleanWritable) w);
    } else if (w instanceof IntWritable) {
      writeInt((IntWritable) w);
    } else if (w instanceof VIntWritable) {
      writeVInt((VIntWritable) w);
    } else if (w instanceof LongWritable) {
      writeLong((LongWritable) w);
    } else if (w instanceof VLongWritable) {
      writeVLong((VLongWritable) w);
    } else if (w instanceof FloatWritable) {
      writeFloat((FloatWritable) w);
    } else if (w instanceof DoubleWritable) {
      writeDouble((DoubleWritable) w);
    } else if (w instanceof Text) {
      writeText((Text) w);
    } else if (w instanceof ArrayWritable) {
      writeArray((ArrayWritable) w);
    } else if (w instanceof MapWritable) {
      writeMap((MapWritable) w);
    } else if (w instanceof SortedMapWritable) {
      writeSortedMap((SortedMapWritable) w);
    } else if (w instanceof Record) {
      writeRecord((Record) w);
    } else {
      writeWritable(w); // last resort
    }
  }

  public void writeTypedBytes(TypedBytesWritable tbw) throws IOException {
    out.writeRaw(tbw.getBytes(), 0, tbw.getLength());
  }

  public void writeBytes(BytesWritable bw) throws IOException {
    byte[] bytes = Arrays.copyOfRange(bw.getBytes(), 0, bw.getLength());
    out.writeBytes(bytes);
  }

  public void writeByte(ByteWritable bw) throws IOException {
    out.writeByte(bw.get());
  }

  public void writeBoolean(BooleanWritable bw) throws IOException {
    out.writeBool(bw.get());
  }

  public void writeInt(IntWritable iw) throws IOException {
    out.writeInt(iw.get());
  }

  public void writeVInt(VIntWritable viw) throws IOException {
    out.writeInt(viw.get());
  }

  public void writeLong(LongWritable lw) throws IOException {
    out.writeLong(lw.get());
  }

  public void writeVLong(VLongWritable vlw) throws IOException {
    out.writeLong(vlw.get());
  }

  public void writeFloat(FloatWritable fw) throws IOException {
    out.writeFloat(fw.get());
  }

  public void writeDouble(DoubleWritable dw) throws IOException {
    out.writeDouble(dw.get());
  }

  public void writeText(Text t) throws IOException {
    out.writeString(t.toString());
  }

  public void writeArray(ArrayWritable aw) throws IOException {
    Writable[] writables = aw.get();
    out.writeVectorHeader(writables.length);
    for (Writable writable : writables) {
      write(writable);
    }
  }

  public void writeMap(MapWritable mw) throws IOException {
    out.writeMapHeader(mw.size());
    for (Map.Entry<Writable, Writable> entry : mw.entrySet()) {
      write(entry.getKey());
      write(entry.getValue());
    }
  }

  public void writeSortedMap(SortedMapWritable smw) throws IOException {
    out.writeMapHeader(smw.size());
    for (Map.Entry<WritableComparable, Writable> entry : smw.entrySet()) {
      write(entry.getKey());
      write(entry.getValue());
    }
  }

  public void writeRecord(Record r) throws IOException {
    r.serialize(TypedBytesRecordOutput.get(out));
  }

  public void writeWritable(Writable w) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    WritableUtils.writeString(dos, w.getClass().getName());
    w.write(dos);
    dos.close();
    out.writeBytes(baos.toByteArray(), Type.WRITABLE.code);
  }

}
