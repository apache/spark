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

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/** A base-class for Writables which store themselves compressed and lazily
 * inflate on field access.  This is useful for large objects whose fields are
 * not be altered during a map or reduce operation: leaving the field data
 * compressed makes copying the instance from one file to another much
 * faster. */
public abstract class CompressedWritable implements Writable {
  // if non-null, the compressed field data of this instance.
  private byte[] compressed;

  public CompressedWritable() {}

  public final void readFields(DataInput in) throws IOException {
    compressed = new byte[in.readInt()];
    in.readFully(compressed, 0, compressed.length);
  }

  /** Must be called by all methods which access fields to ensure that the data
   * has been uncompressed. */
  protected void ensureInflated() {
    if (compressed != null) {
      try {
        ByteArrayInputStream deflated = new ByteArrayInputStream(compressed);
        DataInput inflater =
          new DataInputStream(new InflaterInputStream(deflated));
        readFieldsCompressed(inflater);
        compressed = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Subclasses implement this instead of {@link #readFields(DataInput)}. */
  protected abstract void readFieldsCompressed(DataInput in)
    throws IOException;

  public final void write(DataOutput out) throws IOException {
    if (compressed == null) {
      ByteArrayOutputStream deflated = new ByteArrayOutputStream();
      Deflater deflater = new Deflater(Deflater.BEST_SPEED);
      DataOutputStream dout =
        new DataOutputStream(new DeflaterOutputStream(deflated, deflater));
      writeCompressed(dout);
      dout.close();
      deflater.end();
      compressed = deflated.toByteArray();
    }
    out.writeInt(compressed.length);
    out.write(compressed);
  }

  /** Subclasses implement this instead of {@link #write(DataOutput)}. */
  protected abstract void writeCompressed(DataOutput out) throws IOException;

}
