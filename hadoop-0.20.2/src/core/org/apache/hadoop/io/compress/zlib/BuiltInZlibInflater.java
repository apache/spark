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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * A wrapper around java.util.zip.Inflater to make it conform 
 * to org.apache.hadoop.io.compress.Decompressor interface.
 * 
 */
public class BuiltInZlibInflater extends Inflater implements Decompressor {

  public BuiltInZlibInflater(boolean nowrap) {
    super(nowrap);
  }

  public BuiltInZlibInflater() {
    super();
  }

  public synchronized int decompress(byte[] b, int off, int len) 
    throws IOException {
    try {
      return super.inflate(b, off, len);
    } catch (DataFormatException dfe) {
      throw new IOException(dfe.getMessage());
    }
  }
}
