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
package org.apache.spark.storage;

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStream;

public class ReadPartialFileSystem extends LocalFileSystem {

  public static class ReadPartialInputStream extends InputStream
      implements Seekable, PositionedReadable {
    private final FSDataInputStream in;
    public ReadPartialInputStream(FSDataInputStream in) {
      this.in = in;
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len > 1) {
        return in.read(b, off, len - 1);
      }
      return in.read(b, off, len);
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return in.seekToNewSource(targetPos);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      if (length > 1) {
        return in.read(position, buffer, offset, length - 1);
      }
      return in.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      in.readFully(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      in.readFully(position, buffer);
    }
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    FSDataInputStream stream = super.open(f);
    return new FSDataInputStream(new ReadPartialInputStream(stream));
  }
}
