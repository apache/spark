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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A block-based {@link FileSystem} backed by
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * </p>
 * @see NativeS3FileSystem
 */
public class S3FileSystem extends FileSystem {

  private URI uri;

  private FileSystemStore store;

  private Path workingDir;

  public S3FileSystem() {
    // set store in initialize()
  }
  
  public S3FileSystem(FileSystemStore store) {
    this.store = store;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());    
    this.workingDir =
      new Path("/user", System.getProperty("user.name")).makeQualified(this);
  }  

  private static FileSystemStore createDefaultStore(Configuration conf) {
    FileSystemStore store = new Jets3tFileSystemStore();
    
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                                                               conf.getInt("fs.s3.maxRetries", 4),
                                                                               conf.getLong("fs.s3.sleepTimeSeconds", 10), TimeUnit.SECONDS);
    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(S3Exception.class, basePolicy);
    
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
                                                              RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    methodNameToPolicyMap.put("storeBlock", methodPolicy);
    methodNameToPolicyMap.put("retrieveBlock", methodPolicy);
    
    return (FileSystemStore) RetryProxy.create(FileSystemStore.class,
                                               store, methodNameToPolicyMap);
  }
  
  @Override
  public String getName() {
    return getUri().toString();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(path);
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);
    
    boolean result = true;
    for (Path p : paths) {
      result &= mkdir(p);
    }
    return result;
  }
  
  private boolean mkdir(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      store.storeINode(absolutePath, INode.DIRECTORY_INODE);
    } else if (inode.isFile()) {
      throw new IOException(String.format(
          "Can't make directory for path %s since it is a file.",
          absolutePath));
    }
    return true;
  }

  @Override
  public boolean isFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  private INode checkFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      throw new IOException("No such file.");
    }
    if (inode.isDirectory()) {
      throw new IOException("Path " + path + " is a directory.");
    }
    return inode;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      return null;
    }
    if (inode.isFile()) {
      return new FileStatus[] {
        new S3FileStatus(f.makeQualified(this), inode)
      };
    }
    ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
    for (Path p : store.listSubPaths(absolutePath)) {
      ret.add(getFileStatus(p.makeQualified(this)));
    }
    return ret.toArray(new FileStatus[0]);
  }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
    throws IOException {

    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite) {
        delete(file);
      } else {
        throw new IOException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new IOException("Mkdirs failed to create " + parent.toString());
        }
      }      
    }
    return new FSDataOutputStream
        (new S3OutputStream(getConf(), store, makeAbsolute(file),
                            blockSize, progress, bufferSize),
         statistics);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    INode inode = checkFile(path);
    return new FSDataInputStream(new S3InputStream(getConf(), store, inode,
                                                   statistics));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteSrc = makeAbsolute(src);
    INode srcINode = store.retrieveINode(absoluteSrc);
    if (srcINode == null) {
      // src path doesn't exist
      return false; 
    }
    Path absoluteDst = makeAbsolute(dst);
    INode dstINode = store.retrieveINode(absoluteDst);
    if (dstINode != null && dstINode.isDirectory()) {
      absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
      dstINode = store.retrieveINode(absoluteDst);
    }
    if (dstINode != null) {
      // dst path already exists - can't overwrite
      return false;
    }
    Path dstParent = absoluteDst.getParent();
    if (dstParent != null) {
      INode dstParentINode = store.retrieveINode(dstParent);
      if (dstParentINode == null || dstParentINode.isFile()) {
        // dst parent doesn't exist or is a file
        return false;
      }
    }
    return renameRecursive(absoluteSrc, absoluteDst);
  }
  
  private boolean renameRecursive(Path src, Path dst) throws IOException {
    INode srcINode = store.retrieveINode(src);
    store.storeINode(dst, srcINode);
    store.deleteINode(src);
    if (srcINode.isDirectory()) {
      for (Path oldSrc : store.listDeepSubPaths(src)) {
        INode inode = store.retrieveINode(oldSrc);
        if (inode == null) {
          return false;
        }
        String oldSrcPath = oldSrc.toUri().getPath();
        String srcPath = src.toUri().getPath();
        String dstPath = dst.toUri().getPath();
        Path newDst = new Path(oldSrcPath.replaceFirst(srcPath, dstPath));
        store.storeINode(newDst, inode);
        store.deleteINode(oldSrc);
      }
    }
    return true;
  }

  public boolean delete(Path path, boolean recursive) throws IOException {
   Path absolutePath = makeAbsolute(path);
   INode inode = store.retrieveINode(absolutePath);
   if (inode == null) {
     return false;
   }
   if (inode.isFile()) {
     store.deleteINode(absolutePath);
     for (Block block: inode.getBlocks()) {
       store.deleteBlock(block);
     }
   } else {
     FileStatus[] contents = listStatus(absolutePath);
     if (contents == null) {
       return false;
     }
     if ((contents.length !=0) && (!recursive)) {
       throw new IOException("Directory " + path.toString() 
           + " is not empty.");
     }
     for (FileStatus p:contents) {
       if (!delete(p.getPath(), recursive)) {
         return false;
       }
     }
     store.deleteINode(absolutePath);
   }
   return true;
  }
  
  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  /**
   * FileStatus for S3 file systems. 
   */
  @Override
  public FileStatus getFileStatus(Path f)  throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new FileNotFoundException(f + ": No such file or directory.");
    }
    return new S3FileStatus(f.makeQualified(this), inode);
  }
  
  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong("fs.s3.block.size", 64 * 1024 * 1024);
  }

  // diagnostic methods

  void dump() throws IOException {
    store.dump();
  }

  void purge() throws IOException {
    store.purge();
  }

  private static class S3FileStatus extends FileStatus {

    S3FileStatus(Path f, INode inode) throws IOException {
      super(findLength(inode), inode.isDirectory(), 1,
            findBlocksize(inode), 0, f);
    }

    private static long findLength(INode inode) {
      if (!inode.isDirectory()) {
        long length = 0L;
        for (Block block : inode.getBlocks()) {
          length += block.getLength();
        }
        return length;
      }
      return 0;
    }

    private static long findBlocksize(INode inode) {
      final Block[] ret = inode.getBlocks();
      return ret == null ? 0L : ret[0].getLength();
    }
  }
}
