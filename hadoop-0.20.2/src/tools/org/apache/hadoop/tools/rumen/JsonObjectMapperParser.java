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
package org.apache.hadoop.tools.rumen;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * A simple wrapper for parsing JSON-encoded data using ObjectMapper.
 * @param <T> The (base) type of the object(s) to be parsed by this parser.
 */
class JsonObjectMapperParser<T> implements Closeable {
  private final ObjectMapper mapper;
  private final Class<? extends T> clazz;
  private final JsonParser jsonParser;
  private final Decompressor decompressor;

  /**
   * Constructor.
   * 
   * @param path 
   *          Path to the JSON data file, possibly compressed.
   * @param conf
   * @throws IOException
   */
  public JsonObjectMapperParser(Path path, Class<? extends T> clazz,
      Configuration conf) throws IOException {
    mapper = new ObjectMapper();
    mapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    this.clazz = clazz;
    FileSystem fs = path.getFileSystem(conf);
    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);
    InputStream input;
    if (codec == null) {
      input = fs.open(path);
      decompressor = null;
    } else {
      FSDataInputStream fsdis = fs.open(path);
      decompressor = CodecPool.getDecompressor(codec);
      input = codec.createInputStream(fsdis, decompressor);
    }
    jsonParser = mapper.getJsonFactory().createJsonParser(input);
  }

  /**
   * Constructor.
   * 
   * @param input
   *          The input stream for the JSON data.
   */
  public JsonObjectMapperParser(InputStream input, Class<? extends T> clazz)
      throws IOException {
    mapper = new ObjectMapper();
    mapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    this.clazz = clazz;
    decompressor = null;
    jsonParser = mapper.getJsonFactory().createJsonParser(input);
  }

  /**
   * Get the next object from the trace.
   * 
   * @return The next instance of the object. Or null if we reach the end of
   *         stream.
   * @throws IOException
   */
  public T getNext() throws IOException {
    try {
      return mapper.readValue(jsonParser, clazz);
    } catch (EOFException e) {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      jsonParser.close();
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }
}
