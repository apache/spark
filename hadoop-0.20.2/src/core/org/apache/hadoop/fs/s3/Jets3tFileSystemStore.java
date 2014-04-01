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
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

class Jets3tFileSystemStore implements FileSystemStore {
  
  private static final String FILE_SYSTEM_NAME = "fs";
  private static final String FILE_SYSTEM_VALUE = "Hadoop";

  private static final String FILE_SYSTEM_TYPE_NAME = "fs-type";
  private static final String FILE_SYSTEM_TYPE_VALUE = "block";

  private static final String FILE_SYSTEM_VERSION_NAME = "fs-version";
  private static final String FILE_SYSTEM_VERSION_VALUE = "1";
  
  private static final Map<String, String> METADATA =
    new HashMap<String, String>();
  
  static {
    METADATA.put(FILE_SYSTEM_NAME, FILE_SYSTEM_VALUE);
    METADATA.put(FILE_SYSTEM_TYPE_NAME, FILE_SYSTEM_TYPE_VALUE);
    METADATA.put(FILE_SYSTEM_VERSION_NAME, FILE_SYSTEM_VERSION_VALUE);
  }

  private static final String PATH_DELIMITER = Path.SEPARATOR;
  private static final String BLOCK_PREFIX = "block_";

  private Configuration conf;
  
  private S3Service s3Service;

  private S3Bucket bucket;
  
  private int bufferSize;
  
  public void initialize(URI uri, Configuration conf) throws IOException {
    
    this.conf = conf;
    
    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(uri, conf);
    try {
      AWSCredentials awsCredentials =
        new AWSCredentials(s3Credentials.getAccessKey(),
            s3Credentials.getSecretAccessKey());
      this.s3Service = new RestS3Service(awsCredentials);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
    bucket = new S3Bucket(uri.getHost());

    this.bufferSize = conf.getInt("io.file.buffer.size", 4096);
  }

  public String getVersion() throws IOException {
    return FILE_SYSTEM_VERSION_VALUE;
  }

  private void delete(String key) throws IOException {
    try {
      s3Service.deleteObject(bucket, key);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public void deleteINode(Path path) throws IOException {
    delete(pathToKey(path));
  }

  public void deleteBlock(Block block) throws IOException {
    delete(blockToKey(block));
  }

  public boolean inodeExists(Path path) throws IOException {
    InputStream in = get(pathToKey(path), true);
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }
  
  public boolean blockExists(long blockId) throws IOException {
    InputStream in = get(blockToKey(blockId), false);
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }

  private InputStream get(String key, boolean checkMetadata)
      throws IOException {
    
    try {
      S3Object object = s3Service.getObject(bucket, key);
      if (checkMetadata) {
        checkMetadata(object);
      }
      return object.getDataInputStream();
    } catch (S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  private InputStream get(String key, long byteRangeStart) throws IOException {
    try {
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
                                            null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  private void checkMetadata(S3Object object) throws S3FileSystemException,
      S3ServiceException {
    
    String name = (String) object.getMetadata(FILE_SYSTEM_NAME);
    if (!FILE_SYSTEM_VALUE.equals(name)) {
      throw new S3FileSystemException("Not a Hadoop S3 file.");
    }
    String type = (String) object.getMetadata(FILE_SYSTEM_TYPE_NAME);
    if (!FILE_SYSTEM_TYPE_VALUE.equals(type)) {
      throw new S3FileSystemException("Not a block file.");
    }
    String dataVersion = (String) object.getMetadata(FILE_SYSTEM_VERSION_NAME);
    if (!FILE_SYSTEM_VERSION_VALUE.equals(dataVersion)) {
      throw new VersionMismatchException(FILE_SYSTEM_VERSION_VALUE,
          dataVersion);
    }
  }

  public INode retrieveINode(Path path) throws IOException {
    return INode.deserialize(get(pathToKey(path), true));
  }

  public File retrieveBlock(Block block, long byteRangeStart)
    throws IOException {
    File fileBlock = null;
    InputStream in = null;
    OutputStream out = null;
    try {
      fileBlock = newBackupFile();
      in = get(blockToKey(block), byteRangeStart);
      out = new BufferedOutputStream(new FileOutputStream(fileBlock));
      byte[] buf = new byte[bufferSize];
      int numRead;
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
      return fileBlock;
    } catch (IOException e) {
      // close output stream to file then delete file
      closeQuietly(out);
      out = null; // to prevent a second close
      if (fileBlock != null) {
        fileBlock.delete();
      }
      throw e;
    } finally {
      closeQuietly(out);
      closeQuietly(in);
    }
  }
  
  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create S3 buffer directory: " + dir);
    }
    File result = File.createTempFile("input-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  public Set<Path> listSubPaths(Path path) throws IOException {
    try {
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      S3Object[] objects = s3Service.listObjects(bucket, prefix, PATH_DELIMITER);
      Set<Path> prefixes = new TreeSet<Path>();
      for (int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }
  
  public Set<Path> listDeepSubPaths(Path path) throws IOException {
    try {
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      S3Object[] objects = s3Service.listObjects(bucket, prefix, null);
      Set<Path> prefixes = new TreeSet<Path>();
      for (int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }    
  }

  private void put(String key, InputStream in, long length, boolean storeMetadata)
      throws IOException {
    
    try {
      S3Object object = new S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(length);
      if (storeMetadata) {
        object.addAllMetadata(METADATA);
      }
      s3Service.putObject(bucket, object);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public void storeINode(Path path, INode inode) throws IOException {
    put(pathToKey(path), inode.serialize(), inode.getSerializedLength(), true);
  }

  public void storeBlock(Block block, File file) throws IOException {
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      put(blockToKey(block), in, block.getLength(), false);
    } finally {
      closeQuietly(in);
    }    
  }

  private void closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return path.toUri().getPath();
  }

  private Path keyToPath(String key) {
    return new Path(key);
  }
  
  private String blockToKey(long blockId) {
    return BLOCK_PREFIX + blockId;
  }

  private String blockToKey(Block block) {
    return blockToKey(block.getId());
  }

  public void purge() throws IOException {
    try {
      S3Object[] objects = s3Service.listObjects(bucket);
      for (int i = 0; i < objects.length; i++) {
        s3Service.deleteObject(bucket, objects[i].getKey());
      }
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public void dump() throws IOException {
    StringBuilder sb = new StringBuilder("S3 Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      S3Object[] objects = s3Service.listObjects(bucket, PATH_DELIMITER, null);
      for (int i = 0; i < objects.length; i++) {
        Path path = keyToPath(objects[i].getKey());
        sb.append(path).append("\n");
        INode m = retrieveINode(path);
        sb.append("\t").append(m.getFileType()).append("\n");
        if (m.getFileType() == FileType.DIRECTORY) {
          continue;
        }
        for (int j = 0; j < m.getBlocks().length; j++) {
          sb.append("\t").append(m.getBlocks()[j]).append("\n");
        }
      }
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
    System.out.println(sb);
  }

}
