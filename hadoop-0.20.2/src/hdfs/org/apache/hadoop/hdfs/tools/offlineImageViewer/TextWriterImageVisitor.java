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

import java.io.FileWriter;
import java.io.IOException;

/**
 * TextWriterImageProcessor mixes in the ability for ImageVisitor
 * implementations to easily write their output to a text file.
 *
 * Implementing classes should be sure to call the super methods for the
 * constructors, finish and finishAbnormally methods, in order that the
 * underlying file may be opened and closed correctly.
 *
 * Note, this class does not add newlines to text written to file or (if
 * enabled) screen.  This is the implementing class' responsibility.
 */
abstract class TextWriterImageVisitor extends ImageVisitor {
  private boolean printToScreen = false;
  private boolean okToWrite = false;
  final private FileWriter fw;

  /**
   * Create a processor that writes to the file named.
   *
   * @param filename Name of file to write output to
   */
  public TextWriterImageVisitor(String filename) throws IOException {
    this(filename, false);
  }

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param filename Name of file to write output to
   * @param printToScreen Mirror output to screen?
   */
  public TextWriterImageVisitor(String filename, boolean printToScreen)
         throws IOException {
    super();
    this.printToScreen = printToScreen;
    fw = new FileWriter(filename);
    okToWrite = true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finish()
   */
  @Override
  void finish() throws IOException {
    close();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finishAbnormally()
   */
  @Override
  void finishAbnormally() throws IOException {
    close();
  }

  /**
   * Close output stream and prevent further writing
   */
  private void close() throws IOException {
    fw.close();
    okToWrite = false;
  }

  /**
   * Write parameter to output file (and possibly screen).
   *
   * @param toWrite Text to write to file
   */
  protected void write(String toWrite) throws IOException  {
    if(!okToWrite)
      throw new IOException("file not open for writing.");

    if(printToScreen)
      System.out.print(toWrite);

      try {
        fw.write(toWrite);
      } catch (IOException e) {
        okToWrite = false;
        throw e;
      }
  }
}
