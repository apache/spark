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
package org.apache.spark.internal.io.cloud.abortable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class AbortableFileSystem extends RawLocalFileSystem {

  public static String ABORTABLE_FS_SCHEME = "abortable";

  @Override
  public URI getUri() {
    return URI.create(ABORTABLE_FS_SCHEME + ":///");
  }

  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
    int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    FSDataOutputStream out = this.create(f, overwrite, bufferSize, replication, blockSize,
      progress, permission);
    return out;
  }

  private FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
    long blockSize, Progressable progress, FsPermission permission) throws IOException {
    if (this.exists(f) && !overwrite) {
      throw new FileAlreadyExistsException("File already exists: " + f);
    } else {
      Path parent = f.getParent();
      if (parent != null && !this.mkdirs(parent)) {
        throw new IOException("Mkdirs failed to create " + parent.toString());
      } else {
        return new FSDataOutputStream(this.createOutputStreamWithMode(f, false, permission), null);
      }
    }
  }

  @Override
  protected OutputStream createOutputStreamWithMode(Path f, boolean append,
      FsPermission permission) throws IOException {
    return new AbortableOutputStream(f, append, permission);
  }

  class AbortableOutputStream extends ByteArrayOutputStream
      implements Abortable, StreamCapabilities {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Path f;

    private boolean append;

    private FsPermission permission;

    AbortableOutputStream(Path f, boolean append, FsPermission permission) {
      this.f = f;
      this.append = append;
      this.permission = permission;
    }

    @Override
    public void close() throws IOException {
      if (closed.getAndSet(true)) {
        return;
      }

      OutputStream output =
        AbortableFileSystem.super.createOutputStreamWithMode(f, append, permission);
      writeTo(output);
      output.close();
    }

    @Override
    public AbortableResult abort() {
      final boolean isAlreadyClosed = closed.getAndSet(true);
      return new AbortableResult() {
        public boolean alreadyClosed() {
          return isAlreadyClosed;
        }

        public IOException anyCleanupException() {
          return null;
        }
      };
    }

    @Override
    public boolean hasCapability(String capability) {
      return capability == CommonPathCapabilities.ABORTABLE_STREAM;
    }
  }
}
