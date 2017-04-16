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
 * IndentedImageVisitor walks over an FSImage and displays its structure 
 * using indenting to organize sections within the image file.
 */
class IndentedImageVisitor extends TextWriterImageVisitor {
  
  public IndentedImageVisitor(String filename) throws IOException {
    super(filename);
  }

  public IndentedImageVisitor(String filename, boolean printToScreen) throws IOException {
    super(filename, printToScreen);
  }

  final private DepthCounter dc = new DepthCounter();// to track leading spacing

  @Override
  void start() throws IOException {}

  @Override
  void finish() throws IOException { super.finish(); }

  @Override
  void finishAbnormally() throws IOException {
    System.out.println("*** Image processing finished abnormally.  Ending ***");
    super.finishAbnormally();
  }

  @Override
  void leaveEnclosingElement() throws IOException {
    dc.decLevel();
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    printIndents();
    write(element + " = " + value + "\n");
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    printIndents();
    write(element + "\n");
    dc.incLevel();
  }

  // Print element, along with associated key/value pair, in brackets
  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value)
      throws IOException {
    printIndents();
    write(element + " [" + key + " = " + value + "]\n");
    dc.incLevel();
  }

  /**
  * Print an appropriate number of spaces for the current level.
  * FsImages can potentially be millions of lines long, so caching can
  * significantly speed up output.
  */
  final private static String [] indents = { "",
                                             "  ",
                                             "    ",
                                             "      ",
                                             "        ",
                                             "          ",
                                             "            "};
  private void printIndents() throws IOException {
    try {
      write(indents[dc.getLevel()]);
    } catch (IndexOutOfBoundsException e) {
      // There's no reason in an fsimage would need a deeper indent
      for(int i = 0; i < dc.getLevel(); i++)
        write(" ");
    }
   }
}
