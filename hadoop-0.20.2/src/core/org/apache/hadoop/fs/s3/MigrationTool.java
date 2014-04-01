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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 * <p>
 * This class is a tool for migrating data from an older to a newer version
 * of an S3 filesystem.
 * </p>
 * <p>
 * All files in the filesystem are migrated by re-writing the block metadata
 * - no datafiles are touched.
 * </p>
 */
public class MigrationTool extends Configured implements Tool {
  
  private S3Service s3Service;
  private S3Bucket bucket;
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MigrationTool(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length == 0) {
      System.err.println("Usage: MigrationTool <S3 file system URI>");
      System.err.println("\t<S3 file system URI>\tfilesystem to migrate");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    URI uri = URI.create(args[0]);
    
    initialize(uri);
    
    FileSystemStore newStore = new Jets3tFileSystemStore();
    newStore.initialize(uri, getConf());
    
    if (get("%2F") != null) { 
      System.err.println("Current version number is [unversioned].");
      System.err.println("Target version number is " +
          newStore.getVersion() + ".");
      Store oldStore = new UnversionedStore();
      migrate(oldStore, newStore);
      return 0;
    } else {
      S3Object root = get("/");
      if (root != null) {
        String version = (String) root.getMetadata("fs-version");
        if (version == null) {
          System.err.println("Can't detect version - exiting.");
        } else {
          String newVersion = newStore.getVersion();
          System.err.println("Current version number is " + version + ".");
          System.err.println("Target version number is " + newVersion + ".");
          if (version.equals(newStore.getVersion())) {
            System.err.println("No migration required.");
            return 0;
          }
          // use version number to create Store
          //Store oldStore = ... 
          //migrate(oldStore, newStore);
          System.err.println("Not currently implemented.");
          return 0;
        }
      }
      System.err.println("Can't detect version - exiting.");
      return 0;
    }
    
  }
  
  public void initialize(URI uri) throws IOException {
    
    
    
    try {
      String accessKey = null;
      String secretAccessKey = null;
      String userInfo = uri.getUserInfo();
      if (userInfo != null) {
        int index = userInfo.indexOf(':');
        if (index != -1) {
          accessKey = userInfo.substring(0, index);
          secretAccessKey = userInfo.substring(index + 1);
        } else {
          accessKey = userInfo;
        }
      }
      if (accessKey == null) {
        accessKey = getConf().get("fs.s3.awsAccessKeyId");
      }
      if (secretAccessKey == null) {
        secretAccessKey = getConf().get("fs.s3.awsSecretAccessKey");
      }
      if (accessKey == null && secretAccessKey == null) {
        throw new IllegalArgumentException("AWS " +
                                           "Access Key ID and Secret Access Key " +
                                           "must be specified as the username " +
                                           "or password (respectively) of a s3 URL, " +
                                           "or by setting the " +
                                           "fs.s3.awsAccessKeyId or " +                         
                                           "fs.s3.awsSecretAccessKey properties (respectively).");
      } else if (accessKey == null) {
        throw new IllegalArgumentException("AWS " +
                                           "Access Key ID must be specified " +
                                           "as the username of a s3 URL, or by setting the " +
                                           "fs.s3.awsAccessKeyId property.");
      } else if (secretAccessKey == null) {
        throw new IllegalArgumentException("AWS " +
                                           "Secret Access Key must be specified " +
                                           "as the password of a s3 URL, or by setting the " +
                                           "fs.s3.awsSecretAccessKey property.");         
      }
      AWSCredentials awsCredentials =
        new AWSCredentials(accessKey, secretAccessKey);
      this.s3Service = new RestS3Service(awsCredentials);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
    bucket = new S3Bucket(uri.getHost());
  }
  
  private void migrate(Store oldStore, FileSystemStore newStore)
      throws IOException {
    for (Path path : oldStore.listAllPaths()) {
      INode inode = oldStore.retrieveINode(path);
      oldStore.deleteINode(path);
      newStore.storeINode(path, inode);
    }
  }
  
  private S3Object get(String key) {
    try {
      return s3Service.getObject(bucket, key);
    } catch (S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
    }
    return null;
  }
  
  interface Store {

    Set<Path> listAllPaths() throws IOException;
    INode retrieveINode(Path path) throws IOException;
    void deleteINode(Path path) throws IOException;
    
  }
  
  class UnversionedStore implements Store {

    public Set<Path> listAllPaths() throws IOException {
      try {
        String prefix = urlEncode(Path.SEPARATOR);
        S3Object[] objects = s3Service.listObjects(bucket, prefix, null);
        Set<Path> prefixes = new TreeSet<Path>();
        for (int i = 0; i < objects.length; i++) {
          prefixes.add(keyToPath(objects[i].getKey()));
        }
        return prefixes;
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
    
    public INode retrieveINode(Path path) throws IOException {
      return INode.deserialize(get(pathToKey(path)));
    }

    private InputStream get(String key) throws IOException {
      try {
        S3Object object = s3Service.getObject(bucket, key);
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
    
    private String pathToKey(Path path) {
      if (!path.isAbsolute()) {
        throw new IllegalArgumentException("Path must be absolute: " + path);
      }
      return urlEncode(path.toUri().getPath());
    }
    
    private Path keyToPath(String key) {
      return new Path(urlDecode(key));
    }

    private String urlEncode(String s) {
      try {
        return URLEncoder.encode(s, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        // Should never happen since every implementation of the Java Platform
        // is required to support UTF-8.
        // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
        throw new IllegalStateException(e);
      }
    }
    
    private String urlDecode(String s) {
      try {
        return URLDecoder.decode(s, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        // Should never happen since every implementation of the Java Platform
        // is required to support UTF-8.
        // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
        throw new IllegalStateException(e);
      }
    }
    
  }
  
}
