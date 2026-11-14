/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.kvstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/** Generic fallback with packed index projections and an independently encoded payload. */
class SerializedRecordCodec<T> implements KVStoreRecordCodec<T> {
  private final Class<T> type;
  private final KVStoreSerializer serializer;
  private final KVTypeInfo typeInfo;
  private final List<KVIndex> indices;
  private final Map<String, Integer> ordinals = new HashMap<>();

  SerializedRecordCodec(Class<T> type, KVStoreSerializer serializer) {
    this.type = type;
    this.serializer = serializer;
    this.typeInfo = new KVTypeInfo(type);
    this.indices = typeInfo.indices().toList();
    for (int i = 0; i < indices.size(); i++) {
      ordinals.put(indices.get(i).value(), i);
    }
  }

  @Override
  public Record<T> encode(T value) throws Exception {
    ByteArrayOutputStream indexBytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(indexBytes)) {
      for (KVIndex index : indices) {
        writeIndex(out, typeInfo.getIndexValue(index.value(), value));
      }
    }
    byte[] payload = serializer.serialize(value);
    boolean compressed = false;
    // The JSON serializer already compresses its output; protobuf payloads may benefit here.
    boolean gzip = payload.length > 1 && payload[0] == (byte) 0x1f && payload[1] == (byte) 0x8b;
    if (payload.length >= 256 && !gzip) {
      Deflater deflater = new Deflater(Deflater.BEST_SPEED);
      try {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DeflaterOutputStream out = new DeflaterOutputStream(bytes, deflater)) {
          out.write(payload);
        }
        if (bytes.size() + 16 < payload.length) {
          payload = bytes.toByteArray();
          compressed = true;
        }
      } finally {
        deflater.end();
      }
    }
    return new SerializedRecord(payload, compressed, indexBytes.toByteArray());
  }

  private class SerializedRecord implements Record<T> {
    private final byte[] payload;
    private final boolean compressed;
    private final byte[] indexBytes;

    SerializedRecord(byte[] payload, boolean compressed, byte[] indexBytes) {
      this.payload = payload;
      this.compressed = compressed;
      this.indexBytes = indexBytes;
    }

    @Override
    public T decode() throws Exception {
      if (!compressed) {
        return serializer.deserialize(payload, type);
      }
      try (InflaterInputStream in =
          new InflaterInputStream(new ByteArrayInputStream(payload))) {
        return serializer.deserialize(in.readAllBytes(), type);
      }
    }

    @Override
    public Object indexValue(String name) throws IOException {
      Integer ordinal = ordinals.get(name);
      if (ordinal == null) {
        throw new IllegalArgumentException("No index " + name);
      }
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(indexBytes))) {
        for (int i = 0; i < ordinal; i++) {
          skipIndex(in);
        }
        return readIndex(in);
      }
    }
  }

  private static void writeIndex(DataOutputStream out, Object value) throws IOException {
    if (value == null) {
      out.writeByte(0);
    } else if (value instanceof Boolean v) {
      out.writeByte(1);
      out.writeBoolean(v);
    } else if (value instanceof Byte v) {
      out.writeByte(2);
      out.writeByte(v);
    } else if (value instanceof Short v) {
      out.writeByte(3);
      out.writeShort(v);
    } else if (value instanceof Integer v) {
      out.writeByte(4);
      out.writeInt(v);
    } else if (value instanceof Long v) {
      out.writeByte(5);
      out.writeLong(v);
    } else if (value instanceof String v) {
      out.writeByte(6);
      // Preserve Java's UTF-16 ordering and even unpaired surrogates in indexed strings.
      out.writeInt(v.length());
      for (int i = 0; i < v.length(); i++) {
        out.writeChar(v.charAt(i));
      }
    } else if (value instanceof int[] v) {
      out.writeByte(7);
      out.writeInt(v.length);
      for (int item : v) {
        out.writeInt(item);
      }
    } else if (value instanceof long[] v) {
      out.writeByte(8);
      out.writeInt(v.length);
      for (long item : v) {
        out.writeLong(item);
      }
    } else if (value instanceof byte[] v) {
      out.writeByte(9);
      out.writeInt(v.length);
      out.write(v);
    } else if (value instanceof Object[] v) {
      out.writeByte(10);
      out.writeInt(v.length);
      for (Object item : v) {
        writeIndex(out, item);
      }
    } else {
      throw new IllegalArgumentException("Unsupported index type: " + value.getClass());
    }
  }

  private static Object readIndex(DataInputStream in) throws IOException {
    return switch (in.readByte()) {
      case 0 -> null;
      case 1 -> in.readBoolean();
      case 2 -> in.readByte();
      case 3 -> in.readShort();
      case 4 -> in.readInt();
      case 5 -> in.readLong();
      case 6 -> {
        char[] chars = new char[in.readInt()];
        for (int i = 0; i < chars.length; i++) {
          chars[i] = in.readChar();
        }
        yield new String(chars);
      }
      case 7 -> {
        int[] values = new int[in.readInt()];
        for (int i = 0; i < values.length; i++) {
          values[i] = in.readInt();
        }
        yield values;
      }
      case 8 -> {
        long[] values = new long[in.readInt()];
        for (int i = 0; i < values.length; i++) {
          values[i] = in.readLong();
        }
        yield values;
      }
      case 9 -> in.readNBytes(in.readInt());
      case 10 -> {
        Object[] values = new Object[in.readInt()];
        for (int i = 0; i < values.length; i++) {
          values[i] = readIndex(in);
        }
        yield values;
      }
      default -> throw new IOException("Invalid packed index type");
    };
  }

  private static void skipIndex(DataInputStream in) throws IOException {
    switch (in.readByte()) {
      case 0 -> { }
      case 1, 2 -> in.skipNBytes(1);
      case 3 -> in.skipNBytes(2);
      case 4 -> in.skipNBytes(4);
      case 5 -> in.skipNBytes(8);
      case 6 -> in.skipNBytes(in.readInt() * 2L);
      case 9 -> in.skipNBytes(in.readInt());
      case 7 -> in.skipNBytes(in.readInt() * 4L);
      case 8 -> in.skipNBytes(in.readInt() * 8L);
      case 10 -> {
        int length = in.readInt();
        for (int i = 0; i < length; i++) {
          skipIndex(in);
        }
      }
      default -> throw new IOException("Invalid packed index type");
    }
  }
}
