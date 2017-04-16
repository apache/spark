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

package org.apache.hadoop.fs.s3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;

/**
 * A stub implementation of {@link FileSystemStore} for testing
 * {@link S3FileSystem} without actually connecting to S3.
 */
class InMemoryFileSystemStore implements FileSystemStore {
  
  private Configuration conf;
  private SortedMap<Path, INode> inodes = new TreeMap<Path, INode>();
  private Map<Long, byte[]> blocks = new HashMap<Long, byte[]>();
  
  public void initialize(URI uri, Configuration conf) {
    this.conf = conf;
  }
  
  public String getVersion() throws IOException {
    return "0";
  }

  public void deleteINode(Path path) throws IOException {
    inodes.remove(normalize(path));
  }

  public void deleteBlock(Block block) throws IOException {
    blocks.remove(block.getId());
  }

  public boolean inodeExists(Path path) throws IOException {
    return inodes.containsKey(normalize(path));
  }

  public boolean blockExists(long blockId) throws IOException {
    return blocks.containsKey(blockId);
  }

  public INode retrieveINode(Path path) throws IOException {
    return inodes.get(normalize(path));
  }

  public File retrieveBlock(Block block, long byteRangeStart) throws IOException {
    byte[] data = blocks.get(block.getId());
    File file = createTempFile();
    BufferedOutputStream out = null;
    try {
      out = new BufferedOutputStream(new FileOutputStream(file));
      out.write(data, (int) byteRangeStart, data.length - (int) byteRangeStart);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return file;
  }
  
  private File createTempFile() throws IOException {
    File dir = new File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create S3 buffer directory: " + dir);
    }
    File result = File.createTempFile("test-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  public Set<Path> listSubPaths(Path path) throws IOException {
    Path normalizedPath = normalize(path);
    // This is inefficient but more than adequate for testing purposes.
    Set<Path> subPaths = new LinkedHashSet<Path>();
    for (Path p : inodes.tailMap(normalizedPath).keySet()) {
      if (normalizedPath.equals(p.getParent())) {
        subPaths.add(p);
      }
    }
    return subPaths;
  }

  public Set<Path> listDeepSubPaths(Path path) throws IOException {
    Path normalizedPath = normalize(path);    
    String pathString = normalizedPath.toUri().getPath();
    if (!pathString.endsWith("/")) {
      pathString += "/";
    }
    // This is inefficient but more than adequate for testing purposes.
    Set<Path> subPaths = new LinkedHashSet<Path>();
    for (Path p : inodes.tailMap(normalizedPath).keySet()) {
      if (p.toUri().getPath().startsWith(pathString)) {
        subPaths.add(p);
      }
    }
    return subPaths;
  }

  public void storeINode(Path path, INode inode) throws IOException {
    inodes.put(normalize(path), inode);
  }

  public void storeBlock(Block block, File file) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int numRead;
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
    blocks.put(block.getId(), out.toByteArray());
  }
  
  private Path normalize(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return new Path(path.toUri().getPath());
  }

  public void purge() throws IOException {
    inodes.clear();
    blocks.clear();
  }

  public void dump() throws IOException {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append(", \n");
    for (Map.Entry<Path, INode> entry : inodes.entrySet()) {
      sb.append(entry.getKey()).append("\n");
      INode inode = entry.getValue();
      sb.append("\t").append(inode.getFileType()).append("\n");
      if (inode.getFileType() == FileType.DIRECTORY) {
        continue;
      }
      for (int j = 0; j < inode.getBlocks().length; j++) {
        sb.append("\t").append(inode.getBlocks()[j]).append("\n");
      }      
    }
    System.out.println(sb);
    
    System.out.println(inodes.keySet());
    System.out.println(blocks.keySet());
  }

}
