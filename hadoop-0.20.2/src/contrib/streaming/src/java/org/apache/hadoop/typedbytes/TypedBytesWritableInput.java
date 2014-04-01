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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Provides functionality for reading typed bytes as Writable objects.
 * 
 * @see TypedBytesInput
 */
public class TypedBytesWritableInput implements Configurable {

  private TypedBytesInput in;
  private Configuration conf;

  private TypedBytesWritableInput() {
    conf = new Configuration();
  }

  private void setTypedBytesInput(TypedBytesInput in) {
    this.in = in;
  }

  private static ThreadLocal tbIn = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return new TypedBytesWritableInput();
    }
  };

  /**
   * Get a thread-local typed bytes writable input for the supplied
   * {@link TypedBytesInput}.
   * 
   * @param in typed bytes input object
   * @return typed bytes writable input corresponding to the supplied
   *         {@link TypedBytesInput}.
   */
  public static TypedBytesWritableInput get(TypedBytesInput in) {
    TypedBytesWritableInput bin = (TypedBytesWritableInput) tbIn.get();
    bin.setTypedBytesInput(in);
    return bin;
  }

  /**
   * Get a thread-local typed bytes writable input for the supplied
   * {@link DataInput}.
   * 
   * @param in data input object
   * @return typed bytes writable input corresponding to the supplied
   *         {@link DataInput}.
   */
  public static TypedBytesWritableInput get(DataInput in) {
    return get(TypedBytesInput.get(in));
  }

  /** Creates a new instance of TypedBytesWritableInput. */
  public TypedBytesWritableInput(TypedBytesInput in) {
    this();
    this.in = in;
  }

  /** Creates a new instance of TypedBytesWritableInput. */
  public TypedBytesWritableInput(DataInput din) {
    this(new TypedBytesInput(din));
  }

  public Writable read() throws IOException {
    Type type = in.readType();
    if (type == null) {
      return null;
    }
    switch (type) {
    case BYTES:
      return readBytes();
    case BYTE:
      return readByte();
    case BOOL:
      return readBoolean();
    case INT:
      return readVInt();
    case LONG:
      return readVLong();
    case FLOAT:
      return readFloat();
    case DOUBLE:
      return readDouble();
    case STRING:
      return readText();
    case VECTOR:
      return readArray();
    case MAP:
      return readMap();
    case WRITABLE:
      return readWritable();
    default:
      throw new RuntimeException("unknown type");
    }
  }

  public Class<? extends Writable> readType() throws IOException {
    Type type = in.readType();
    if (type == null) {
      return null;
    }
    switch (type) {
    case BYTES:
      return BytesWritable.class;
    case BYTE:
      return ByteWritable.class;
    case BOOL:
      return BooleanWritable.class;
    case INT:
      return VIntWritable.class;
    case LONG:
      return VLongWritable.class;
    case FLOAT:
      return FloatWritable.class;
    case DOUBLE:
      return DoubleWritable.class;
    case STRING:
      return Text.class;
    case VECTOR:
      return ArrayWritable.class;
    case MAP:
      return MapWritable.class;
    case WRITABLE:
      return Writable.class;
    default:
      throw new RuntimeException("unknown type");
    }
  }

  public BytesWritable readBytes(BytesWritable bw) throws IOException {
    byte[] bytes = in.readBytes();
    if (bw == null) {
      bw = new BytesWritable(bytes);
    } else {
      bw.set(bytes, 0, bytes.length);
    }
    return bw;
  }

  public BytesWritable readBytes() throws IOException {
    return readBytes(null);
  }

  public ByteWritable readByte(ByteWritable bw) throws IOException {
    if (bw == null) {
      bw = new ByteWritable();
    }
    bw.set(in.readByte());
    return bw;
  }

  public ByteWritable readByte() throws IOException {
    return readByte(null);
  }

  public BooleanWritable readBoolean(BooleanWritable bw) throws IOException {
    if (bw == null) {
      bw = new BooleanWritable();
    }
    bw.set(in.readBool());
    return bw;
  }

  public BooleanWritable readBoolean() throws IOException {
    return readBoolean(null);
  }

  public IntWritable readInt(IntWritable iw) throws IOException {
    if (iw == null) {
      iw = new IntWritable();
    }
    iw.set(in.readInt());
    return iw;
  }

  public IntWritable readInt() throws IOException {
    return readInt(null);
  }

  public VIntWritable readVInt(VIntWritable iw) throws IOException {
    if (iw == null) {
      iw = new VIntWritable();
    }
    iw.set(in.readInt());
    return iw;
  }

  public VIntWritable readVInt() throws IOException {
    return readVInt(null);
  }

  public LongWritable readLong(LongWritable lw) throws IOException {
    if (lw == null) {
      lw = new LongWritable();
    }
    lw.set(in.readLong());
    return lw;
  }

  public LongWritable readLong() throws IOException {
    return readLong(null);
  }

  public VLongWritable readVLong(VLongWritable lw) throws IOException {
    if (lw == null) {
      lw = new VLongWritable();
    }
    lw.set(in.readLong());
    return lw;
  }

  public VLongWritable readVLong() throws IOException {
    return readVLong(null);
  }

  public FloatWritable readFloat(FloatWritable fw) throws IOException {
    if (fw == null) {
      fw = new FloatWritable();
    }
    fw.set(in.readFloat());
    return fw;
  }

  public FloatWritable readFloat() throws IOException {
    return readFloat(null);
  }

  public DoubleWritable readDouble(DoubleWritable dw) throws IOException {
    if (dw == null) {
      dw = new DoubleWritable();
    }
    dw.set(in.readDouble());
    return dw;
  }

  public DoubleWritable readDouble() throws IOException {
    return readDouble(null);
  }

  public Text readText(Text t) throws IOException {
    if (t == null) {
      t = new Text();
    }
    t.set(in.readString());
    return t;
  }

  public Text readText() throws IOException {
    return readText(null);
  }

  public ArrayWritable readArray(ArrayWritable aw) throws IOException {
    if (aw == null) {
      aw = new ArrayWritable(TypedBytesWritable.class);
    } else if (!aw.getValueClass().equals(TypedBytesWritable.class)) {
      throw new RuntimeException("value class has to be TypedBytesWritable");
    }
    int length = in.readVectorHeader();
    Writable[] writables = new Writable[length];
    for (int i = 0; i < length; i++) {
      writables[i] = new TypedBytesWritable(in.readRaw());
    }
    aw.set(writables);
    return aw;
  }

  public ArrayWritable readArray() throws IOException {
    return readArray(null);
  }

  public MapWritable readMap(MapWritable mw) throws IOException {
    if (mw == null) {
      mw = new MapWritable();
    }
    int length = in.readMapHeader();
    for (int i = 0; i < length; i++) {
      Writable key = read();
      Writable value = read();
      mw.put(key, value);
    }
    return mw;
  }

  public MapWritable readMap() throws IOException {
    return readMap(null);
  }

  public SortedMapWritable readSortedMap(SortedMapWritable mw)
    throws IOException {
    if (mw == null) {
      mw = new SortedMapWritable();
    }
    int length = in.readMapHeader();
    for (int i = 0; i < length; i++) {
      WritableComparable key = (WritableComparable) read();
      Writable value = read();
      mw.put(key, value);
    }
    return mw;
  }

  public SortedMapWritable readSortedMap() throws IOException {
    return readSortedMap(null);
  }
  
  public Writable readWritable(Writable writable) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(in.readBytes());
    DataInputStream dis = new DataInputStream(bais);
    String className = WritableUtils.readString(dis);
    if (writable == null) {
      try {
        Class<? extends Writable> cls = 
          conf.getClassByName(className).asSubclass(Writable.class);
        writable = (Writable) ReflectionUtils.newInstance(cls, conf);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    } else if (!writable.getClass().getName().equals(className)) {
      throw new IOException("wrong Writable class given");
    }
    writable.readFields(dis);
    return writable;
  }

  public Writable readWritable() throws IOException {
    return readWritable(null);
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
}
