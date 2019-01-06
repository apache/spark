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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.annotation.Private;

/**
 * Serializer used to translate between app-defined types and the LevelDB store.
 *
 * <p>
 * The serializer is based on Jackson, so values are written as JSON. It also allows "naked strings"
 * and integers to be written as values directly, which will be written as UTF-8 strings.
 * </p>
 */
@Private
public class KVStoreSerializer {

  /**
   * Object mapper used to process app-specific types. If an application requires a specific
   * configuration of the mapper, it can subclass this serializer and add custom configuration
   * to this object.
   */
  protected final ObjectMapper mapper;

  public KVStoreSerializer() {
    this.mapper = new ObjectMapper();
  }

  public final byte[] serialize(Object o) throws Exception {
    if (o instanceof String) {
      return ((String) o).getBytes(UTF_8);
    } else {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
        mapper.writeValue(out, o);
      }
      return bytes.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  public final <T> T deserialize(byte[] data, Class<T> klass) throws Exception {
    if (klass.equals(String.class)) {
      return (T) new String(data, UTF_8);
    } else {
      try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(data))) {
        return mapper.readValue(in, klass);
      }
    }
  }

  final byte[] serialize(long value) {
    return String.valueOf(value).getBytes(UTF_8);
  }

  final long deserializeLong(byte[] data) {
    return Long.parseLong(new String(data, UTF_8));
  }

}
