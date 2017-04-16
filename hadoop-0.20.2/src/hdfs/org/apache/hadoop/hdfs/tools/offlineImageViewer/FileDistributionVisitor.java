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
import java.util.LinkedList;

/**
 * File size distribution visitor.
 * 
 * <h3>Description.</h3>
 * This is the tool for analyzing file sizes in the namespace image.
 * In order to run the tool one should define a range of integers
 * <tt>[0, maxSize]</tt> by specifying <tt>maxSize</tt> and a <tt>step</tt>.
 * The range of integers is divided into segments of size <tt>step</tt>: 
 * <tt>[0, s<sub>1</sub>, ..., s<sub>n-1</sub>, maxSize]</tt>,
 * and the visitor calculates how many files in the system fall into 
 * each segment <tt>[s<sub>i-1</sub>, s<sub>i</sub>)</tt>. 
 * Note that files larger than <tt>maxSize</tt> always fall into 
 * the very last segment.
 * 
 * <h3>Input.</h3>
 * <ul>
 * <li><tt>filename</tt> specifies the location of the image file;</li>
 * <li><tt>maxSize</tt> determines the range <tt>[0, maxSize]</tt> of files
 * sizes considered by the visitor;</li>
 * <li><tt>step</tt> the range is divided into segments of size step.</li>
 * </ul>
 *
 * <h3>Output.</h3>
 * The output file is formatted as a tab separated two column table:
 * Size and NumFiles. Where Size represents the start of the segment,
 * and numFiles is the number of files form the image which size falls in 
 * this segment.
 */
class FileDistributionVisitor extends TextWriterImageVisitor {
  final private LinkedList<ImageElement> elemS = new LinkedList<ImageElement>();

  private final static long MAX_SIZE_DEFAULT = 0x2000000000L;   // 1/8 TB = 2^37
  private final static int INTERVAL_DEFAULT = 0x200000;         // 2 MB = 2^21

  private int[] distribution;
  private long maxSize;
  private int step;

  private int totalFiles;
  private int totalDirectories;
  private int totalBlocks;
  private long totalSpace;
  private long maxFileSize;

  private FileContext current;

  private boolean inInode = false;

  /**
   * File or directory information.
   */
  private static class FileContext {
    String path;
    long fileSize;
    int numBlocks;
    int replication;
  }

  public FileDistributionVisitor(String filename,
                                 long maxSize,
                                 int step) throws IOException {
    super(filename, false);
    this.maxSize = (maxSize == 0 ? MAX_SIZE_DEFAULT : maxSize);
    this.step = (step == 0 ? INTERVAL_DEFAULT : step);
    long numIntervals = this.maxSize / this.step;
    if(numIntervals >= Integer.MAX_VALUE)
      throw new IOException("Too many distribution intervals " + numIntervals);
    this.distribution = new int[1 + (int)(numIntervals)];
    this.totalFiles = 0;
    this.totalDirectories = 0;
    this.totalBlocks = 0;
    this.totalSpace = 0;
    this.maxFileSize = 0;
  }

  @Override
  void start() throws IOException {}

  @Override
  void finish() throws IOException {
    // write the distribution into the output file
    write("Size\tNumFiles\n");
    for(int i = 0; i < distribution.length; i++)
      write(((long)i * step) + "\t" + distribution[i] + "\n");
    System.out.println("totalFiles = " + totalFiles);
    System.out.println("totalDirectories = " + totalDirectories);
    System.out.println("totalBlocks = " + totalBlocks);
    System.out.println("totalSpace = " + totalSpace);
    System.out.println("maxFileSize = " + maxFileSize);
    super.finish();
  }

  @Override
  void leaveEnclosingElement() throws IOException {
    ImageElement elem = elemS.pop();

    if(elem != ImageElement.INODE &&
       elem != ImageElement.INODE_UNDER_CONSTRUCTION)
      return;
    inInode = false;
    if(current.numBlocks < 0) {
      totalDirectories ++;
      return;
    }
    totalFiles++;
    totalBlocks += current.numBlocks;
    totalSpace += current.fileSize * current.replication;
    if(maxFileSize < current.fileSize)
      maxFileSize = current.fileSize;
    int high;
    if(current.fileSize > maxSize)
      high = distribution.length-1;
    else
      high = (int)Math.ceil((double)current.fileSize / step);
    distribution[high]++;
    if(totalFiles % 1000000 == 1)
      System.out.println("Files processed: " + totalFiles
          + "  Current: " + current.path);
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    if(inInode) {
      switch(element) {
      case INODE_PATH:
        current.path = (value.equals("") ? "/" : value);
        break;
      case REPLICATION:
        current.replication = Integer.valueOf(value);
        break;
      case NUM_BYTES:
        current.fileSize += Long.valueOf(value);
        break;
      default:
        break;
      }
    }
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    elemS.push(element);
    if(element == ImageElement.INODE ||
       element == ImageElement.INODE_UNDER_CONSTRUCTION) {
      current = new FileContext();
      inInode = true;
    }
  }

  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value) throws IOException {
    elemS.push(element);
    if(element == ImageElement.INODE ||
       element == ImageElement.INODE_UNDER_CONSTRUCTION)
      inInode = true;
    else if(element == ImageElement.BLOCKS)
      current.numBlocks = Integer.parseInt(value);
  }
}
