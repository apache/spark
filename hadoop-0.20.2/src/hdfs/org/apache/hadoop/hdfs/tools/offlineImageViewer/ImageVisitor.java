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

/**
 * An implementation of ImageVisitor can traverse the structure of an
 * Hadoop fsimage and respond to each of the structures within the file.
 */
abstract class ImageVisitor {

  /**
   * Structural elements of an FSImage that may be encountered within the
   * file. ImageVisitors are able to handle processing any of these elements.
   */
  public enum ImageElement {
    FS_IMAGE,
    IMAGE_VERSION,
    NAMESPACE_ID,
    LAYOUT_VERSION,
    NUM_INODES,
    GENERATION_STAMP,
    INODES,
    INODE,
    INODE_PATH,
    REPLICATION,
    MODIFICATION_TIME,
    ACCESS_TIME,
    BLOCK_SIZE,
    NUM_BLOCKS,
    BLOCKS,
    BLOCK,
    BLOCK_ID,
    NUM_BYTES,
    NS_QUOTA,
    DS_QUOTA,
    PERMISSIONS,
    NUM_INODES_UNDER_CONSTRUCTION,
    INODES_UNDER_CONSTRUCTION,
    INODE_UNDER_CONSTRUCTION,
    PREFERRED_BLOCK_SIZE,
    CLIENT_NAME,
    CLIENT_MACHINE,
    USER_NAME,
    GROUP_NAME,
    PERMISSION_STRING
  }
  
  /**
   * Utility class for tracking descent into the structure of the fsimage.
   */
  static protected class DepthCounter {
    private int depth = 0;

    public void incLevel() { depth++; }
    public void decLevel() { if(depth >= 1) depth--; }
    public int  getLevel() { return depth; }
  }

  /**
   * Begin visiting the fsimage structure.  Opportunity to perform
   * any initialization necessary for the implementing visitor.
   */
  abstract void start() throws IOException;

  /**
   * Finish visiting the fsimage structure.  Opportunity to perform any
   * clean up necessary for the implementing visitor.
   */
  abstract void finish() throws IOException;

  /**
   * Finish visiting the fsimage structure after an error has occurred
   * during the processing.  Opportunity to perform any clean up necessary
   * for the implementing visitor.
   */
  abstract void finishAbnormally() throws IOException;

  /**
   * Visit non enclosing element of fsimage with specified value.
   *
   * @param element FSImage element
   * @param value Element's value
   */
  abstract void visit(ImageElement element, String value) throws IOException;

  // Convenience methods to automatically convert numeric value types to strings
  void visit(ImageElement element, int value) throws IOException {
    visit(element, Integer.toString(value));
  }

  void visit(ImageElement element, long value) throws IOException {
    visit(element, Long.toString(value));
  }

  /**
   * Begin visiting an element that encloses another element, such as
   * the beginning of the list of blocks that comprise a file.
   *
   * @param element Element being visited
   */
  abstract void visitEnclosingElement(ImageElement element)
     throws IOException;

  /**
   * Begin visiting an element that encloses another element, such as
   * the beginning of the list of blocks that comprise a file.
   *
   * Also provide an additional key and value for the element, such as the
   * number items within the element.
   *
   * @param element Element being visited
   * @param key Key describing the element being visited
   * @param value Value associated with element being visited
   */
  abstract void visitEnclosingElement(ImageElement element,
      ImageElement key, String value) throws IOException;

  // Convenience methods to automatically convert value types to strings
  void visitEnclosingElement(ImageElement element,
      ImageElement key, int value)
     throws IOException {
    visitEnclosingElement(element, key, Integer.toString(value));
  }

  void visitEnclosingElement(ImageElement element,
      ImageElement key, long value)
     throws IOException {
    visitEnclosingElement(element, key, Long.toString(value));
  }

  /**
   * Leave current enclosing element.  Called, for instance, at the end of
   * processing the blocks that compromise a file.
   */
  abstract void leaveEnclosingElement() throws IOException;
}
