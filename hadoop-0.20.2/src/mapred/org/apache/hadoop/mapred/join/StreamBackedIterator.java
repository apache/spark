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
package org.apache.hadoop.mapred.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class provides an implementation of ResetableIterator. This
 * implementation uses a byte array to store elements added to it.
 */
public class StreamBackedIterator<X extends Writable>
    implements ResetableIterator<X> {

  private static class ReplayableByteInputStream extends ByteArrayInputStream {
    public ReplayableByteInputStream(byte[] arr) {
      super(arr);
    }
    public void resetStream() {
      mark = 0;
      reset();
    }
  }

  private ByteArrayOutputStream outbuf = new ByteArrayOutputStream();
  private DataOutputStream outfbuf = new DataOutputStream(outbuf);
  private ReplayableByteInputStream inbuf;
  private DataInputStream infbuf;

  public StreamBackedIterator() { }

  public boolean hasNext() {
    return infbuf != null && inbuf.available() > 0;
  }

  public boolean next(X val) throws IOException {
    if (hasNext()) {
      inbuf.mark(0);
      val.readFields(infbuf);
      return true;
    }
    return false;
  }

  public boolean replay(X val) throws IOException {
    inbuf.reset();
    if (0 == inbuf.available())
      return false;
    val.readFields(infbuf);
    return true;
  }

  public void reset() {
    if (null != outfbuf) {
      inbuf = new ReplayableByteInputStream(outbuf.toByteArray());
      infbuf =  new DataInputStream(inbuf);
      outfbuf = null;
    }
    inbuf.resetStream();
  }

  public void add(X item) throws IOException {
    item.write(outfbuf);
  }

  public void close() throws IOException {
    if (null != infbuf)
      infbuf.close();
    if (null != outfbuf)
      outfbuf.close();
  }

  public void clear() {
    if (null != inbuf)
      inbuf.resetStream();
    outbuf.reset();
    outfbuf = new DataOutputStream(outbuf);
  }
}
