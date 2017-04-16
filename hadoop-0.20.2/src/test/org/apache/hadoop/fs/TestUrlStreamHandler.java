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
package org.apache.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

/**
 * Test of the URL stream handler factory.
 */
public class TestUrlStreamHandler extends TestCase {

  /**
   * Test opening and reading from an InputStream through a hdfs:// URL.
   * <p>
   * First generate a file with some content through the FileSystem API, then
   * try to open and read the file through the URL stream API.
   * 
   * @throws IOException
   */
  public void testDfsUrls() throws IOException {

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();

    // Setup our own factory
    // setURLSteramHandlerFactor is can be set at most once in the JVM
    // the new URLStreamHandler is valid for all tests cases 
    // in TestStreamHandler
    FsUrlStreamHandlerFactory factory =
        new org.apache.hadoop.fs.FsUrlStreamHandlerFactory();
    java.net.URL.setURLStreamHandlerFactory(factory);

    Path filePath = new Path("/thefile");

    try {
      byte[] fileContent = new byte[1024];
      for (int i = 0; i < fileContent.length; ++i)
        fileContent[i] = (byte) i;

      // First create the file through the FileSystem API
      OutputStream os = fs.create(filePath);
      os.write(fileContent);
      os.close();

      // Second, open and read the file content through the URL API
      URI uri = fs.getUri();
      URL fileURL =
          new URL(uri.getScheme(), uri.getHost(), uri.getPort(), filePath
              .toString());

      InputStream is = fileURL.openStream();
      assertNotNull(is);

      byte[] bytes = new byte[4096];
      assertEquals(1024, is.read(bytes));
      is.close();

      for (int i = 0; i < fileContent.length; ++i)
        assertEquals(fileContent[i], bytes[i]);

      // Cleanup: delete the file
      fs.delete(filePath, false);

    } finally {
      fs.close();
      cluster.shutdown();
    }

  }

  /**
   * Test opening and reading from an InputStream through a file:// URL.
   * 
   * @throws IOException
   * @throws URISyntaxException
   */
  public void testFileUrls() throws IOException, URISyntaxException {
    // URLStreamHandler is already set in JVM by testDfsUrls() 
    Configuration conf = new Configuration();

    // Locate the test temporary directory.
    File tmpDir = new File(conf.get("hadoop.tmp.dir"));
    if (!tmpDir.exists()) {
      if (!tmpDir.mkdirs())
        throw new IOException("Cannot create temporary directory: " + tmpDir);
    }

    File tmpFile = new File(tmpDir, "thefile");
    URI uri = tmpFile.toURI();

    FileSystem fs = FileSystem.get(uri, conf);

    try {
      byte[] fileContent = new byte[1024];
      for (int i = 0; i < fileContent.length; ++i)
        fileContent[i] = (byte) i;

      // First create the file through the FileSystem API
      OutputStream os = fs.create(new Path(uri.getPath()));
      os.write(fileContent);
      os.close();

      // Second, open and read the file content through the URL API.
      URL fileURL = uri.toURL();

      InputStream is = fileURL.openStream();
      assertNotNull(is);

      byte[] bytes = new byte[4096];
      assertEquals(1024, is.read(bytes));
      is.close();

      for (int i = 0; i < fileContent.length; ++i)
        assertEquals(fileContent[i], bytes[i]);

      // Cleanup: delete the file
      fs.delete(new Path(uri.getPath()), false);

    } finally {
      fs.close();
    }

  }

}
