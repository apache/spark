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
import java.util.Formatter;
import java.util.LinkedList;

/**
 * LsImageVisitor displays the blocks of the namespace in a format very similar
 * to the output of ls/lsr.  Entries are marked as directories or not,
 * permissions listed, replication, username and groupname, along with size,
 * modification date and full path.
 *
 * Note: A significant difference between the output of the lsr command
 * and this image visitor is that this class cannot sort the file entries;
 * they are listed in the order they are stored within the fsimage file. 
 * Therefore, the output of this class cannot be directly compared to the
 * output of the lsr command.
 */
class LsImageVisitor extends TextWriterImageVisitor {
  final private LinkedList<ImageElement> elemQ = new LinkedList<ImageElement>();

  private int numBlocks;
  private String perms;
  private int replication;
  private String username;
  private String group;
  private long filesize;
  private String modTime;
  private String path;

  private boolean inInode = false;
  final private StringBuilder sb = new StringBuilder();
  final private Formatter formatter = new Formatter(sb);

  public LsImageVisitor(String filename) throws IOException {
    super(filename);
  }

  public LsImageVisitor(String filename, boolean printToScreen) throws IOException {
    super(filename, printToScreen);
  }

  /**
   * Start a new line of output, reset values.
   */
  private void newLine() {
    numBlocks = 0;
    perms = username = group = path = "";
    filesize = 0l;
    replication = 0;

    inInode = true;
  }

  /**
   * All the values have been gathered.  Print them to the console in an
   * ls-style format.
   */
  private final static int widthRepl = 2;  
  private final static int widthUser = 8; 
  private final static int widthGroup = 10; 
  private final static int widthSize = 10;
  private final static int widthMod = 10;
  private final static String lsStr = " %" + widthRepl + "s %" + widthUser + 
                                       "s %" + widthGroup + "s %" + widthSize +
                                       "d %" + widthMod + "s %s";
  private void printLine() throws IOException {
    sb.append(numBlocks < 0 ? "d" : "-");
    sb.append(perms);

    formatter.format(lsStr, replication > 0 ? replication : "-",
                           username, group, filesize, modTime, path);
    sb.append("\n");

    write(sb.toString());
    sb.setLength(0); // clear string builder

    inInode = false;
  }

  @Override
  void start() throws IOException {}

  @Override
  void finish() throws IOException {
    super.finish();
  }

  @Override
  void finishAbnormally() throws IOException {
    System.out.println("Input ended unexpectedly.");
    super.finishAbnormally();
  }

  @Override
  void leaveEnclosingElement() throws IOException {
    ImageElement elem = elemQ.pop();

    if(elem == ImageElement.INODE)
      printLine();
  }

  // Maintain state of location within the image tree and record
  // values needed to display the inode in ls-style format.
  @Override
  void visit(ImageElement element, String value) throws IOException {
    if(inInode) {
      switch(element) {
      case INODE_PATH:
        if(value.equals("")) path = "/";
        else path = value;
        break;
      case PERMISSION_STRING:
        perms = value;
        break;
      case REPLICATION:
        replication = Integer.valueOf(value);
        break;
      case USER_NAME:
        username = value;
        break;
      case GROUP_NAME:
        group = value;
        break;
      case NUM_BYTES:
        filesize += Long.valueOf(value);
        break;
      case MODIFICATION_TIME:
        modTime = value;
        break;
      default:
        // This is OK.  We're not looking for all the values.
        break;
      }
    }
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    elemQ.push(element);
    if(element == ImageElement.INODE)
      newLine();
  }

  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value) throws IOException {
    elemQ.push(element);
    if(element == ImageElement.INODE)
      newLine();
    else if (element == ImageElement.BLOCKS)
      numBlocks = Integer.valueOf(value);
  }
}
