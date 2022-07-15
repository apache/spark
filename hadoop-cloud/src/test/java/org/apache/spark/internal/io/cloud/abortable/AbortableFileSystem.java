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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

public class AbortableFileSystem extends RawLocalFileSystem {

  public static String ABORTABLE_FS_SCHEME = "abortable";

  @Override
  public URI getUri() {
    return URI.create(ABORTABLE_FS_SCHEME + ":///");
  }

  @Override
  protected OutputStream createOutputStreamWithMode(Path f, boolean append,
      FsPermission permission) throws IOException {
    return new AbortableOutputStream(f, append, permission);
  }

  class AbortableOutputStream extends ByteArrayOutputStream implements Abortable {

    private Path f;

    private boolean append;

    private FsPermission permission;

    AbortableOutputStream(Path f, boolean append, FsPermission permission) {
      this.f = f;
      this.append = append;
      this.permission = permission;
    }

    public void close() throws IOException {
      OutputStream output =
        AbortableFileSystem.super.createOutputStreamWithMode(f, append, permission);
      writeTo(output);
      output.close();
    }

    public AbortableResult abort() {
      return new AbortableResult() {
        public boolean alreadyClosed() {
          return false;
        }

        public IOException anyCleanupException() {
          return null;
        }
      };
    }
  }
}
