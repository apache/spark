/*
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

package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class encapsulates a streaming compression/decompression pair.
 */
public interface CompressionCodec {

  /**
   * Create a {@link CompressionOutputStream} that will write to the given 
   * {@link OutputStream}.
   * 
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  CompressionOutputStream createOutputStream(OutputStream out) 
  throws IOException;
  
  /**
   * Create a {@link CompressionOutputStream} that will write to the given 
   * {@link OutputStream} with the given {@link Compressor}.
   * 
   * @param out the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  CompressionOutputStream createOutputStream(OutputStream out, 
                                             Compressor compressor) 
  throws IOException;

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   * 
   * @return the type of compressor needed by this codec.
   */
  Class<? extends Compressor> getCompressorType();
  
  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   * 
   * @return a new compressor for use by this codec
   */
  Compressor createCompressor();
  
  /**
   * Create a stream decompressor that will read from the given input stream.
   * 
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  CompressionInputStream createInputStream(InputStream in) throws IOException;
  
  /**
   * Create a {@link CompressionInputStream} that will read from the given 
   * {@link InputStream} with the given {@link Decompressor}.
   * 
   * @param in the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  CompressionInputStream createInputStream(InputStream in, 
                                           Decompressor decompressor) 
  throws IOException;


  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   * 
   * @return the type of decompressor needed by this codec.
   */
  Class<? extends Decompressor> getDecompressorType();
  
  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   * 
   * @return a new decompressor for use by this codec
   */
  Decompressor createDecompressor();
  
  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  String getDefaultExtension();
}
