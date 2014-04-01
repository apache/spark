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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hdfs.tools.offlineImageViewer.SpotCheckImageVisitor.ImageInfo;

import junit.framework.TestCase;

public class TestOIVCanReadOldVersions extends TestCase {
  // Location of fsimage files during testing.
  public static final String TEST_CACHE_DATA_DIR =
    System.getProperty("test.cache.data", "build/test/cache");
  
  // Verify that the image processor can correctly process prior Hadoop
  // layout versions.  These fsimages were previously generated and stored
  // with the test.  Test success indicates that no changes have been made
  // to the OIV that causes older fsimages to be incorrectly processed.
  public void testOldFSImages() {
    // Define the expected values from the prior versions, as they were created
    // and verified at time of creation
    Set<String> pathNames = new HashSet<String>();
    Collections.addAll(pathNames, "", /* root */
                                  "/bar",
                                  "/bar/dir0",
                                  "/bar/dir0/file0",
                                  "/bar/dir0/file1",
                                  "/bar/dir1",
                                  "/bar/dir1/file0",
                                  "/bar/dir1/file1",
                                  "/bar/dir2",
                                  "/bar/dir2/file0",
                                  "/bar/dir2/file1",
                                  "/foo",
                                  "/foo/dir0",
                                  "/foo/dir0/file0",
                                  "/foo/dir0/file1",
                                  "/foo/dir0/file2",
                                  "/foo/dir0/file3",
                                  "/foo/dir1",
                                  "/foo/dir1/file0",
                                  "/foo/dir1/file1",
                                  "/foo/dir1/file2",
                                  "/foo/dir1/file3");
    
    Set<String> INUCpaths = new HashSet<String>();
    Collections.addAll(INUCpaths, "/bar/dir0/file0",
                                  "/bar/dir0/file1",
                                  "/bar/dir1/file0",
                                  "/bar/dir1/file1",
                                  "/bar/dir2/file0",
                                  "/bar/dir2/file1");
    
    ImageInfo v18Inodes = new ImageInfo(); // Hadoop version 18 inodes
    v18Inodes.totalNumBlocks = 12;
    v18Inodes.totalFileSize = 1069548540l;
    v18Inodes.pathNames = pathNames;
    v18Inodes.totalReplications = 14;
    
    ImageInfo v18INUCs = new ImageInfo(); // Hadoop version 18 inodes under construction
    v18INUCs.totalNumBlocks = 0;
    v18INUCs.totalFileSize = 0;
    v18INUCs.pathNames = INUCpaths;
    v18INUCs.totalReplications = 6;
    
    ImageInfo v19Inodes = new ImageInfo(); // Hadoop version 19 inodes
    v19Inodes.totalNumBlocks = 12;
    v19Inodes.totalFileSize = 1069548540l;
    v19Inodes.pathNames = pathNames;
    v19Inodes.totalReplications = 14;
    
    ImageInfo v19INUCs = new ImageInfo(); // Hadoop version 19 inodes under construction
    v19INUCs.totalNumBlocks = 0;
    v19INUCs.totalFileSize = 0;
    v19INUCs.pathNames = INUCpaths;
    v19INUCs.totalReplications = 6;
    

    spotCheck("18", TEST_CACHE_DATA_DIR + "/fsimageV18", v18Inodes, v18INUCs);
    spotCheck("19", TEST_CACHE_DATA_DIR + "/fsimageV19", v19Inodes, v19INUCs);
  }

  // Check that running the processor now gives us the same values as before
  private void spotCheck(String hadoopVersion, String input, 
       ImageInfo inodes, ImageInfo INUCs) {
    SpotCheckImageVisitor v = new SpotCheckImageVisitor();
    OfflineImageViewer oiv = new OfflineImageViewer(input, v, false);
    try {
      oiv.go();
    } catch (IOException e) {
      fail("Error processing file: " + input);
    }

    compareSpotCheck(hadoopVersion, v.getINodesInfo(), inodes);
    compareSpotCheck(hadoopVersion, v.getINUCsInfo(), INUCs);
    System.out.println("Successfully processed fsimage file from Hadoop version " +
                                                    hadoopVersion);
  }

  // Compare the spot check results of what we generated from the image
  // processor and what we expected to receive
  private void compareSpotCheck(String hadoopVersion, 
                     ImageInfo generated, ImageInfo expected) {
    assertEquals("Version " + hadoopVersion + ": Same number of total blocks", 
                     expected.totalNumBlocks, generated.totalNumBlocks);
    assertEquals("Version " + hadoopVersion + ": Same total file size", 
                     expected.totalFileSize, generated.totalFileSize);
    assertEquals("Version " + hadoopVersion + ": Same total replication factor", 
                     expected.totalReplications, generated.totalReplications);
    assertEquals("Version " + hadoopVersion + ": One-to-one matching of path names", 
                     expected.pathNames, generated.pathNames);
  }
}
