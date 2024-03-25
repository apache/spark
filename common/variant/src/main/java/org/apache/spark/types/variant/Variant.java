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

package org.apache.spark.types.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.CharArrayWriter;
import java.io.IOException;

import static org.apache.spark.types.variant.VariantUtil.*;

/**
 * This class is structurally equivalent to {@link org.apache.spark.unsafe.types.VariantVal}. We
 * define a new class to avoid depending on or modifying Spark.
 */
public final class Variant {
  private final byte[] value;
  private final byte[] metadata;

  public Variant(byte[] value, byte[] metadata) {
    this.value = value;
    this.metadata = metadata;
    // There is currently only one allowed version.
    if (metadata.length < 1 || (metadata[0] & VERSION_MASK) != VERSION) {
      throw malformedVariant();
    }
    // Don't attempt to use a Variant larger than 16 MiB. We'll never produce one, and it risks
    // memory instability.
    if (metadata.length > SIZE_LIMIT || value.length > SIZE_LIMIT) {
      throw variantConstructorSizeLimit();
    }
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getMetadata() {
    return metadata;
  }

  // Stringify the variant in JSON format.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public String toJson() {
    StringBuilder sb = new StringBuilder();
    toJsonImpl(value, metadata, 0, sb);
    return sb.toString();
  }

  // Escape a string so that it can be pasted into JSON structure.
  // For example, if `str` only contains a new-line character, then the result content is "\n"
  // (4 characters).
  static String escapeJson(String str) {
    try (CharArrayWriter writer = new CharArrayWriter();
         JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      gen.writeString(str);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static void toJsonImpl(byte[] value, byte[] metadata, int pos, StringBuilder sb) {
    switch (VariantUtil.getType(value, pos)) {
      case OBJECT:
        handleObject(value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
          sb.append('{');
          for (int i = 0; i < size; ++i) {
            int id = readUnsigned(value, idStart + idSize * i, idSize);
            int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
            int elementPos = dataStart + offset;
            if (i != 0) sb.append(',');
            sb.append(escapeJson(getMetadataKey(metadata, id)));
            sb.append(':');
            toJsonImpl(value, metadata, elementPos, sb);
          }
          sb.append('}');
          return null;
        });
        break;
      case ARRAY:
        handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> {
          sb.append('[');
          for (int i = 0; i < size; ++i) {
            int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
            int elementPos = dataStart + offset;
            if (i != 0) sb.append(',');
            toJsonImpl(value, metadata, elementPos, sb);
          }
          sb.append(']');
          return null;
        });
        break;
      case NULL:
        sb.append("null");
        break;
      case BOOLEAN:
        sb.append(VariantUtil.getBoolean(value, pos));
        break;
      case LONG:
        sb.append(VariantUtil.getLong(value, pos));
        break;
      case STRING:
        sb.append(escapeJson(VariantUtil.getString(value, pos)));
        break;
      case DOUBLE:
        sb.append(VariantUtil.getDouble(value, pos));
        break;
      case DECIMAL:
        sb.append(VariantUtil.getDecimal(value, pos).toPlainString());
        break;
    }
  }
}
