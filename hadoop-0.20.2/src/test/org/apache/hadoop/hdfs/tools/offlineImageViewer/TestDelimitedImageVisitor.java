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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor.ImageElement;

/**
 * Test that the DelimitedImageVisistor gives the expected output based
 * on predetermined inputs
 */
public class TestDelimitedImageVisitor extends TestCase {
  private static String ROOT = System.getProperty("test.build.data","/tmp");
  private static final String delim = "--";
  
  // Record an element in the visitor and build the expected line in the output
  private void build(DelimitedImageVisitor div, ImageElement elem, String val, 
                     StringBuilder sb, boolean includeDelim) throws IOException {
    div.visit(elem, val);
    sb.append(val);
    
    if(includeDelim)
      sb.append(delim);
  }
  
  public void testDelimitedImageVisistor() {
    String filename = ROOT + "/testDIV";
    File f = new File(filename);
    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();
    
    try {
      DelimitedImageVisitor div = new DelimitedImageVisitor(filename, true, delim);

      div.visit(ImageElement.FS_IMAGE, "Not in ouput");
      div.visitEnclosingElement(ImageElement.INODE);
      div.visit(ImageElement.LAYOUT_VERSION, "not in");
      div.visit(ImageElement.LAYOUT_VERSION, "the output");
      
      build(div, ImageElement.INODE_PATH,        "hartnell", sb, true);
      build(div, ImageElement.REPLICATION,       "99", sb, true);
      build(div, ImageElement.MODIFICATION_TIME, "troughton", sb, true);
      build(div, ImageElement.ACCESS_TIME,       "pertwee", sb, true);
      build(div, ImageElement.BLOCK_SIZE,        "baker", sb, true);
      build(div, ImageElement.NUM_BLOCKS,        "davison", sb, true);
      build(div, ImageElement.NUM_BYTES,         "55", sb, true);
      build(div, ImageElement.NS_QUOTA,          "baker2", sb, true);
      build(div, ImageElement.DS_QUOTA,          "mccoy", sb, true);
      build(div, ImageElement.PERMISSION_STRING, "eccleston", sb, true);
      build(div, ImageElement.USER_NAME,         "tennant", sb, true);
      build(div, ImageElement.GROUP_NAME,        "smith", sb, false);
      
      div.leaveEnclosingElement(); // INode
      div.finish();
      
      br = new BufferedReader(new FileReader(f));
      String actual = br.readLine();
      
      // Should only get one line
      assertNull(br.readLine());
      br.close();
      
      String exepcted = sb.toString();
      System.out.println("Expect to get: " + exepcted);
      System.out.println("Actually got:  " + actual);
      assertEquals(exepcted, actual);
      
    } catch (IOException e) {
      fail("Error while testing delmitedImageVisitor" + e.getMessage());
    } finally {
      if(f.exists())
        f.delete();
    }
  }
}
