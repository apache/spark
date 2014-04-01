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

package org.apache.hadoop.contrib.index.lucene;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

/**
 * The initial version of an index is stored in a read-only FileSystem dir
 * (FileSystemDirectory). Index files created by newer versions are written to
 * a writable local FS dir (Lucene's FSDirectory). We should use the general
 * FileSystemDirectory for the writable dir as well. But have to use Lucene's
 * FSDirectory because currently Lucene does randome write and
 * FileSystemDirectory only supports sequential write.
 * 
 * Note: We may delete files from the read-only FileSystem dir because there
 * can be some segment files from an uncommitted checkpoint. For the same
 * reason, we may create files in the writable dir which already exist in the
 * read-only dir and logically they overwrite the ones in the read-only dir.
 */
class MixedDirectory extends Directory {

  private final Directory readDir; // FileSystemDirectory
  private final Directory writeDir; // Lucene's FSDirectory

  // take advantage of the fact that Lucene's FSDirectory.fileExists is faster

  public MixedDirectory(FileSystem readFs, Path readPath, FileSystem writeFs,
      Path writePath, Configuration conf) throws IOException {

    try {
      readDir = new FileSystemDirectory(readFs, readPath, false, conf);
      // check writeFS is a local FS?
      writeDir = FSDirectory.getDirectory(writePath.toString());

    } catch (IOException e) {
      try {
        close();
      } catch (IOException e1) {
        // ignore this one, throw the original one
      }
      throw e;
    }

    lockFactory = new NoLockFactory();
  }

  // for debugging
  MixedDirectory(Directory readDir, Directory writeDir) throws IOException {
    this.readDir = readDir;
    this.writeDir = writeDir;

    lockFactory = new NoLockFactory();
  }

  @Override
  public String[] list() throws IOException {
    String[] readFiles = readDir.list();
    String[] writeFiles = writeDir.list();

    if (readFiles == null || readFiles.length == 0) {
      return writeFiles;
    } else if (writeFiles == null || writeFiles.length == 0) {
      return readFiles;
    } else {
      String[] result = new String[readFiles.length + writeFiles.length];
      System.arraycopy(readFiles, 0, result, 0, readFiles.length);
      System.arraycopy(writeFiles, 0, result, readFiles.length,
          writeFiles.length);
      return result;
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    if (writeDir.fileExists(name)) {
      writeDir.deleteFile(name);
    }
    if (readDir.fileExists(name)) {
      readDir.deleteFile(name);
    }
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return writeDir.fileExists(name) || readDir.fileExists(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (writeDir.fileExists(name)) {
      return writeDir.fileLength(name);
    } else {
      return readDir.fileLength(name);
    }
  }

  @Override
  public long fileModified(String name) throws IOException {
    if (writeDir.fileExists(name)) {
      return writeDir.fileModified(name);
    } else {
      return readDir.fileModified(name);
    }
  }

  @Override
  public void renameFile(String from, String to) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void touchFile(String name) throws IOException {
    if (writeDir.fileExists(name)) {
      writeDir.touchFile(name);
    } else {
      readDir.touchFile(name);
    }
  }

  @Override
  public IndexOutput createOutput(String name) throws IOException {
    return writeDir.createOutput(name);
  }

  @Override
  public IndexInput openInput(String name) throws IOException {
    if (writeDir.fileExists(name)) {
      return writeDir.openInput(name);
    } else {
      return readDir.openInput(name);
    }
  }

  @Override
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    if (writeDir.fileExists(name)) {
      return writeDir.openInput(name, bufferSize);
    } else {
      return readDir.openInput(name, bufferSize);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (readDir != null) {
        readDir.close();
      }
    } finally {
      if (writeDir != null) {
        writeDir.close();
      }
    }
  }

  public String toString() {
    return this.getClass().getName() + "@" + readDir + "&" + writeDir;
  }

}
