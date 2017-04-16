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
import java.util.HashSet;
import java.util.Set;

/**
 * ImageVisitor to spot check an fsimage and generate several statistics
 * about it that we can compare with known values to give a reasonable
 * assertion that the image was processed correctly.
 */
class SpotCheckImageVisitor extends ImageVisitor {
  
  // Statistics gathered by the visitor for Inodes and InodesUnderConstruction
  static public class ImageInfo {
    public long totalNumBlocks = 0; // Total number of blocks in section
    public Set<String> pathNames = new HashSet<String>(); // All path names
    public long totalFileSize = 0; // Total size of all the files
    public long totalReplications = 0; // Sum of all the replications
  }

  final private ImageInfo inodes = new ImageInfo();
  final private ImageInfo INUCs = new ImageInfo();
  private ImageInfo current = null;
  
  @Override
  void visit(ImageElement element, String value) throws IOException {
    if(element == ImageElement.NUM_BYTES) 
      current.totalFileSize += Long.valueOf(value);
    else if (element == ImageElement.REPLICATION)
      current.totalReplications += Long.valueOf(value);
    else if (element == ImageElement.INODE_PATH)
      current.pathNames.add(value);
  }

  @Override
  void visitEnclosingElement(ImageElement element, ImageElement key,
      String value) throws IOException {
    switch(element) {
    case INODES:
      current = inodes;
      break;
    case INODES_UNDER_CONSTRUCTION:
      current = INUCs;
      break;
    case BLOCKS:
      current.totalNumBlocks += Long.valueOf(value);
      break;
      // OK to not have a default, we're skipping most of the values
    }
  }
  
  public ImageInfo getINodesInfo() { return inodes; }
  
  public ImageInfo getINUCsInfo() { return INUCs; }
  
  // Unnecessary visitor methods
  @Override
  void finish() throws IOException {}

  @Override
  void finishAbnormally() throws IOException {}

  @Override
  void leaveEnclosingElement() throws IOException {}

  @Override
  void start() throws IOException {}

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {}
}
