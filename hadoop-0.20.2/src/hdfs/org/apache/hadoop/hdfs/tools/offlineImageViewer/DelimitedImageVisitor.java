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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A DelimitedImageVisitor generates a text representation of the fsimage,
 * with each element separated by a delimiter string.  All of the elements
 * common to both inodes and inodes-under-construction are included. When 
 * processing an fsimage with a layout version that did not include an 
 * element, such as AccessTime, the output file will include a column
 * for the value, but no value will be included.
 * 
 * Individual block information for each file is not currently included.
 * 
 * The default delimiter is tab, as this is an unlikely value to be included
 * an inode path or other text metadata.  The delimiter value can be via the
 * constructor.
 */
class DelimitedImageVisitor extends TextWriterImageVisitor {
  private static final String defaultDelimiter = "\t"; 
  
  final private LinkedList<ImageElement> elemQ = new LinkedList<ImageElement>();
  private long fileSize = 0l;
  // Elements of fsimage we're interested in tracking
  private final Collection<ImageElement> elementsToTrack;
  // Values for each of the elements in elementsToTrack
  private final AbstractMap<ImageElement, String> elements = 
                                            new HashMap<ImageElement, String>();
  private final String delimiter;

  {
    elementsToTrack = new ArrayList<ImageElement>();
    
    // This collection determines what elements are tracked and the order
    // in which they are output
    Collections.addAll(elementsToTrack,  ImageElement.INODE_PATH,
                                         ImageElement.REPLICATION,
                                         ImageElement.MODIFICATION_TIME,
                                         ImageElement.ACCESS_TIME,
                                         ImageElement.BLOCK_SIZE,
                                         ImageElement.NUM_BLOCKS,
                                         ImageElement.NUM_BYTES,
                                         ImageElement.NS_QUOTA,
                                         ImageElement.DS_QUOTA,
                                         ImageElement.PERMISSION_STRING,
                                         ImageElement.USER_NAME,
                                         ImageElement.GROUP_NAME);
  }
  
  public DelimitedImageVisitor(String filename) throws IOException {
    this(filename, false);
  }

  public DelimitedImageVisitor(String outputFile, boolean printToScreen) 
                                                           throws IOException {
    this(outputFile, printToScreen, defaultDelimiter);
  }
  
  public DelimitedImageVisitor(String outputFile, boolean printToScreen, 
                               String delimiter) throws IOException {
    super(outputFile, printToScreen);
    this.delimiter = delimiter;
    reset();
  }

  /**
   * Reset the values of the elements we're tracking in order to handle
   * the next file
   */
  private void reset() {
    elements.clear();
    for(ImageElement e : elementsToTrack) 
      elements.put(e, null);
    
    fileSize = 0l;
  }
  
  @Override
  void leaveEnclosingElement() throws IOException {
    ImageElement elem = elemQ.pop();

    // If we're done with an inode, write out our results and start over
    if(elem == ImageElement.INODE || 
       elem == ImageElement.INODE_UNDER_CONSTRUCTION) {
      writeLine();
      write("\n");
      reset();
    }
  }

  /**
   * Iterate through all the elements we're tracking and, if a value was
   * recorded for it, write it out.
   */
  private void writeLine() throws IOException {
    Iterator<ImageElement> it = elementsToTrack.iterator();
    
    while(it.hasNext()) {
      ImageElement e = it.next();
      
      String v = null;
      if(e == ImageElement.NUM_BYTES)
        v = String.valueOf(fileSize);
      else
        v = elements.get(e);
      
      if(v != null)
        write(v);
      
      if(it.hasNext())
        write(delimiter);
    }
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    // Explicitly label the root path
    if(element == ImageElement.INODE_PATH && value.equals(""))
      value = "/";
    
    // Special case of file size, which is sum of the num bytes in each block
    if(element == ImageElement.NUM_BYTES)
      fileSize += Long.valueOf(value);
    
    if(elements.containsKey(element) && element != ImageElement.NUM_BYTES)
      elements.put(element, value);
    
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    elemQ.push(element);
  }

  @Override
  void visitEnclosingElement(ImageElement element, ImageElement key,
      String value) throws IOException {
    // Special case as numBlocks is an attribute of the blocks element
    if(key == ImageElement.NUM_BLOCKS 
        && elements.containsKey(ImageElement.NUM_BLOCKS))
      elements.put(key, value);
    
    elemQ.push(element);
  }
  
  @Override
  void start() throws IOException { /* Nothing to do */ }
}
