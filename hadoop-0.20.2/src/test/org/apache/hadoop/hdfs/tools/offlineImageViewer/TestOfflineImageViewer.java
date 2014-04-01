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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;

/**
 * Test function of OfflineImageViewer by:
 *   * confirming it can correctly process a valid fsimage file and that
 *     the processing generates a correct representation of the namespace
 *   * confirming it correctly fails to process an fsimage file with a layout
 *     version it shouldn't be able to handle
 *   * confirm it correctly bails on malformed image files, in particular, a
 *     file that ends suddenly.
 */
public class TestOfflineImageViewer extends TestCase {
  private static final int NUM_DIRS = 3;
  private static final int FILES_PER_DIR = 4;

  // Elements of lines of ls-file output to be compared to FileStatus instance
  private class LsElements {
    public String perms;
    public int replication;
    public String username;
    public String groupname;
    public long filesize;
    public char dir; // d if dir, - otherwise
  }
  
  // namespace as written to dfs, to be compared with viewer's output
  final HashMap<String, FileStatus> writtenFiles 
                                           = new HashMap<String, FileStatus>();
  
  
  private static String ROOT = System.getProperty("test.build.data",
                                                  "build/test/data");
  
  // Main entry point into testing.  Necessary since we only want to generate
  // the fsimage file once and use it for multiple tests. 
  public void testOIV() {
    File originalFsimage = null;
    try {
    originalFsimage = initFsimage();
    assertNotNull("originalFsImage shouldn't be null", originalFsimage);
    
    // Tests:
    outputOfLSVisitor(originalFsimage);
    outputOfFileDistributionVisitor(originalFsimage);
    
    unsupportedFSLayoutVersion(originalFsimage);
    
    truncatedFSImage(originalFsimage);
    
    } finally {
      if(originalFsimage != null && originalFsimage.exists())
        originalFsimage.delete();
    }
  }

  // Create a populated namespace for later testing.  Save its contents to a
  // data structure and store its fsimage location.
  private File initFsimage() {
    MiniDFSCluster cluster = null;
    File orig = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 4, true, null);
      FileSystem hdfs = cluster.getFileSystem();
      
      int filesize = 256;
      
      // Create a reasonable namespace 
      for(int i = 0; i < NUM_DIRS; i++)  {
        Path dir = new Path("/dir" + i);
        hdfs.mkdirs(dir);
        writtenFiles.put(dir.toString(), pathToFileEntry(hdfs, dir.toString()));
        for(int j = 0; j < FILES_PER_DIR; j++) {
          Path file = new Path(dir, "file" + j);
          FSDataOutputStream o = hdfs.create(file);
          o.write(new byte[ filesize++ ]);
          o.close();
          
          writtenFiles.put(file.toString(), pathToFileEntry(hdfs, file.toString()));
        }
      }

      // Write results to the fsimage file
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNode().saveNamespace();
      
      // Determine location of fsimage file
      File [] files = cluster.getNameDirs().toArray(new File[0]);
      orig =  new File(files[0], "current/fsimage");
      
      if(!orig.exists())
        fail("Didn't generate or can't find fsimage.");

    } catch (IOException e) {
      fail("Failed trying to generate fsimage file: " + e.getMessage());
    } finally {
      if(cluster != null)
        cluster.shutdown();
    }
    return orig;
  }
  
  // Convenience method to generate a file status from file system for 
  // later comparison
  private FileStatus pathToFileEntry(FileSystem hdfs, String file) 
        throws IOException {
    return hdfs.getFileStatus(new Path(file));
  }

  // Verify that we can correctly generate an ls-style output for a valid 
  // fsimage
  private void outputOfLSVisitor(File originalFsimage) {
    File testFile = new File(ROOT, "/basicCheck");
    File outputFile = new File(ROOT, "/basicCheckOutput");
    
    try {
      copyFile(originalFsimage, testFile);
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);

      oiv.go();
      
      HashMap<String, LsElements> fileOutput = readLsfile(outputFile);
      
      compareNamespaces(writtenFiles, fileOutput);
    } catch (IOException e) {
      fail("Failed reading valid file: " + e.getMessage());
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
    System.out.println("Correctly generated ls-style output.");
  }
  
  // Confirm that attempting to read an fsimage file with an unsupported
  // layout results in an error
  public void unsupportedFSLayoutVersion(File originalFsimage) {
    File testFile = new File(ROOT, "/invalidLayoutVersion");
    File outputFile = new File(ROOT, "invalidLayoutVersionOutput");
    
    try {
      int badVersionNum = -432;
      changeLayoutVersion(originalFsimage, testFile, badVersionNum);
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);
      
      try {
        oiv.go();
        fail("Shouldn't be able to read invalid laytout version");
      } catch(IOException e) {
        if(!e.getMessage().contains(Integer.toString(badVersionNum)))
          throw e; // wasn't error we were expecting
        System.out.println("Correctly failed at reading bad image version.");
      }
    } catch (IOException e) {
      fail("Problem testing unsupported layout version: " + e.getMessage());
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
  }
  
  // Verify that image viewer will bail on a file that ends unexpectedly
  private void truncatedFSImage(File originalFsimage) {
    File testFile = new File(ROOT, "/truncatedFSImage");
    File outputFile = new File(ROOT, "/trucnatedFSImageOutput");
    try {
      copyPartOfFile(originalFsimage, testFile);
      assertTrue("Created truncated fsimage", testFile.exists());
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);

      try {
        oiv.go();
        fail("Managed to process a truncated fsimage file");
      } catch (EOFException e) {
        System.out.println("Correctly handled EOF");
      }
      
    } catch (IOException e) {
      fail("Failed testing truncatedFSImage: " + e.getMessage());
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
  }
  
  // Test that our ls file has all the same compenents of the original namespace
  private void compareNamespaces(HashMap<String, FileStatus> written,
      HashMap<String, LsElements> fileOutput) {
    assertEquals( "Should be the same number of files in both, plus one for root"
            + " in fileoutput", fileOutput.keySet().size(), 
                                written.keySet().size() + 1);
    Set<String> inFile = fileOutput.keySet();

    // For each line in the output file, verify that the namespace had a
    // filestatus counterpart 
    for (String path : inFile) {
      if (path.equals("/")) // root's not included in output from system call
        continue;

      assertTrue("Path in file (" + path + ") was written to fs", written
          .containsKey(path));
      
      compareFiles(written.get(path), fileOutput.get(path));
      
      written.remove(path);
    }

    assertEquals("No more files were written to fs", 0, written.size());
  }
  
  // Compare two files as listed in the original namespace FileStatus and
  // the output of the ls file from the image processor
  private void compareFiles(FileStatus fs, LsElements elements) {
    assertEquals("directory listed as such",  
                                        fs.isDir() ? 'd' : '-', elements.dir);
    assertEquals("perms string equal", 
                                fs.getPermission().toString(), elements.perms);
    assertEquals("replication equal", fs.getReplication(), elements.replication);
    assertEquals("owner equal", fs.getOwner(), elements.username);
    assertEquals("group equal", fs.getGroup(), elements.groupname);
    assertEquals("lengths equal", fs.getLen(), elements.filesize);
  }

  // Read the contents of the file created by the Ls processor
  private HashMap<String, LsElements> readLsfile(File lsFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(lsFile));
    String line = null;
    HashMap<String, LsElements> fileContents = new HashMap<String, LsElements>();
    
    while((line = br.readLine()) != null) 
      readLsLine(line, fileContents);
    
    return fileContents;
  }
  
  // Parse a line from the ls output.  Store permissions, replication, 
  // username, groupname and filesize in hashmap keyed to the path name
  private void readLsLine(String line, HashMap<String, LsElements> fileContents) {
    String elements [] = line.split("\\s+");
    
    assertEquals("Not enough elements in ls output", 8, elements.length);
    
    LsElements lsLine = new LsElements();
    
    lsLine.dir = elements[0].charAt(0);
    lsLine.perms = elements[0].substring(1);
    lsLine.replication = elements[1].equals("-") 
                                             ? 0 : Integer.valueOf(elements[1]);
    lsLine.username = elements[2];
    lsLine.groupname = elements[3];
    lsLine.filesize = Long.valueOf(elements[4]);
    // skipping date and time 
    
    String path = elements[7];
    
    // Check that each file in the ls output was listed once
    assertFalse("LS file had duplicate file entries", 
        fileContents.containsKey(path));
    
    fileContents.put(path, lsLine);
  }
  
  // Copy one fsimage to another, changing the layout version in the process
  private void changeLayoutVersion(File src, File dest, int newVersion) 
         throws IOException {
    DataInputStream in = null; 
    DataOutputStream out = null; 
    
    try {
      in = new DataInputStream(new FileInputStream(src));
      out = new DataOutputStream(new FileOutputStream(dest));
      
      in.readInt();
      out.writeInt(newVersion);
      
      byte [] b = new byte[1024];
      while( in.read(b)  > 0 ) {
        out.write(b);
      }
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }
  
  // Only copy part of file into the other.  Used for testing truncated fsimage
  private void copyPartOfFile(File src, File dest) throws IOException {
    InputStream in = null;
    OutputStream out = null;
    
    byte [] b = new byte[256];
    int bytesWritten = 0;
    int count;
    int maxBytes = 700;
    
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);
      
      while( (count = in.read(b))  > 0 && bytesWritten < maxBytes ) {
        out.write(b);
        bytesWritten += count;
      } 
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }
  
  // Copy one file's contents into the other
  private void copyFile(File src, File dest) throws IOException {
    InputStream in = null;
    OutputStream out = null;
    
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);

      byte [] b = new byte[1024];
      while( in.read(b)  > 0 ) {
        out.write(b);
      }
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }

  private void outputOfFileDistributionVisitor(File originalFsimage) {
    File testFile = new File(ROOT, "/basicCheck");
    File outputFile = new File(ROOT, "/fileDistributionCheckOutput");

    int totalFiles = 0;
    try {
      copyFile(originalFsimage, testFile);
      ImageVisitor v = new FileDistributionVisitor(outputFile.getPath(), 0, 0);
      OfflineImageViewer oiv = 
        new OfflineImageViewer(testFile.getPath(), v, false);

      oiv.go();

      BufferedReader reader = new BufferedReader(new FileReader(outputFile));
      String line = reader.readLine();
      assertEquals(line, "Size\tNumFiles");
      while((line = reader.readLine()) != null) {
        String[] row = line.split("\t");
        assertEquals(row.length, 2);
        totalFiles += Integer.parseInt(row[1]);
      }
    } catch (IOException e) {
      fail("Failed reading valid file: " + e.getMessage());
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
    assertEquals(totalFiles, NUM_DIRS * FILES_PER_DIR);
  }
}
