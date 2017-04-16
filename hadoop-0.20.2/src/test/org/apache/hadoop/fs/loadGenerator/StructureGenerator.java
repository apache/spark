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

package org.apache.hadoop.fs.loadGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.util.ToolRunner;

/**
 * This program generates a random namespace structure with the following
 * constraints:
 * 1. The number of subdirectories is a random number in [minWidth, maxWidth].
 * 2. The maximum depth of each subdirectory is a random number 
 *    [2*maxDepth/3, maxDepth].
 * 3. Files are randomly placed in the empty directories. The size of each
 *    file follows Gaussian distribution.
 * The generated namespace structure is described by two files in the output
 * directory. Each line of the first file 
 * contains the full name of a leaf directory.  
 * Each line of the second file contains
 * the full name of a file and its size, separated by a blank.
 * 
 * The synopsis of the command is
 * java StructureGenerator
    -maxDepth <maxDepth> : maximum depth of the directory tree; default is 5.
    -minWidth <minWidth> : minimum number of subdirectories per directories; default is 1
    -maxWidth <maxWidth> : maximum number of subdirectories per directories; default is 5
    -numOfFiles <#OfFiles> : the total number of files; default is 10.
    -avgFileSize <avgFileSizeInBlocks>: average size of blocks; default is 1.
    -outDir <outDir>: output directory; default is the current directory.
    -seed <seed>: random number generator seed; default is the current time.
 */
public class StructureGenerator {
  private int maxDepth = 5;
  private int minWidth = 1;
  private int maxWidth = 5;
  private int numOfFiles = 10;
  private double avgFileSize = 1;
  private File outDir = DEFAULT_STRUCTURE_DIRECTORY;
  final static private String USAGE = "java StructureGenerator\n" +
  	"-maxDepth <maxDepth>\n" +
    "-minWidth <minWidth>\n" +
    "-maxWidth <maxWidth>\n" +
    "-numOfFiles <#OfFiles>\n" +
    "-avgFileSize <avgFileSizeInBlocks>\n" +
    "-outDir <outDir>\n" +
    "-seed <seed>";
  
  private Random r = null; 
  
  /** Default directory for storing file/directory structure */
  final static File DEFAULT_STRUCTURE_DIRECTORY = new File(".");
  /** The name of the file for storing directory structure */
  final static String DIR_STRUCTURE_FILE_NAME = "dirStructure";
  /** The name of the file for storing file structure */
  final static String FILE_STRUCTURE_FILE_NAME = "fileStructure";
  /** The name prefix for the files created by this program */
  final static String FILE_NAME_PREFIX = "_file_";
  
  /**
   * The main function first parses the command line arguments,
   * then generates in-memory directory structure and outputs to a file,
   * last generates in-memory files and outputs them to a file.
   */
  public int run(String[] args) throws Exception {
    int exitCode = 0;
    exitCode = init(args);
    if (exitCode != 0) {
      return exitCode;
    }
    genDirStructure();
    output(new File(outDir, DIR_STRUCTURE_FILE_NAME));
    genFileStructure();
    outputFiles(new File(outDir, FILE_STRUCTURE_FILE_NAME));
    return exitCode;
  }

  /** Parse the command line arguments and initialize the data */
  private int init(String[] args) {
    try {
      for (int i = 0; i < args.length; i++) { // parse command line
        if (args[i].equals("-maxDepth")) {
          maxDepth = Integer.parseInt(args[++i]);
          if (maxDepth<1) {
            System.err.println("maxDepth must be positive: " + maxDepth);
            return -1;
          }
        } else if (args[i].equals("-minWidth")) {
          minWidth = Integer.parseInt(args[++i]);
          if (minWidth<0) {
            System.err.println("minWidth must be positive: " + minWidth);
            return -1;
          }
        } else if (args[i].equals("-maxWidth")) {
          maxWidth = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-numOfFiles")) {
          numOfFiles = Integer.parseInt(args[++i]);
          if (numOfFiles<1) {
            System.err.println("NumOfFiles must be positive: " + numOfFiles);
            return -1;
          }
        } else if (args[i].equals("-avgFileSize")) {
          avgFileSize = Double.parseDouble(args[++i]);
          if (avgFileSize<=0) {
            System.err.println("AvgFileSize must be positive: " + avgFileSize);
            return -1;
          }
        } else if (args[i].equals("-outDir")) {
          outDir = new File(args[++i]);
        } else if (args[i].equals("-seed")) {
          r = new Random(Long.parseLong(args[++i]));
        } else {
          System.err.println(USAGE);
          ToolRunner.printGenericCommandUsage(System.err);
          return -1;
        }
      }
    } catch (NumberFormatException e) {
      System.err.println("Illegal parameter: " + e.getLocalizedMessage());
      System.err.println(USAGE);
      return -1;
    }
    
    if (maxWidth < minWidth) {
      System.err.println(
          "maxWidth must be bigger than minWidth: " + maxWidth);
      return -1;
    }
    
    if (r==null) {
      r = new Random();
    }
    return 0;
  }
  
  /** In memory representation of a directory */
  private static class INode {
    private String name;
    private List<INode> children = new ArrayList<INode>();
    
    /** Constructor */
    private INode(String name) {
      this.name = name;
    }
    
    /** Add a child (subdir/file) */
    private void addChild(INode child) {
      children.add(child);
    }
    
    /** Output the subtree rooted at the current node. 
     * Only the leaves are printed.
     */
    private void output(PrintStream out, String prefix) {
      prefix = prefix==null?name:prefix+"/"+name;
      if (children.isEmpty()) {
        out.println(prefix);
      } else {
        for (INode child : children) {
          child.output(out, prefix);
        }
      }
    }
    
    /** Output the files in the subtree rooted at this node */
    protected void outputFiles(PrintStream out, String prefix) {
      prefix = prefix==null?name:prefix+"/"+name;
      for (INode child : children) {
        child.outputFiles(out, prefix);
      }
    }
    
    /** Add all the leaves in the subtree to the input list */
    private void getLeaves(List<INode> leaves) {
      if (children.isEmpty()) {
        leaves.add(this);
      } else {
        for (INode child : children) {
          child.getLeaves(leaves);
        }
      }
    }
  }
  
  /** In memory representation of a file */
  private static class FileINode extends INode {
    private double numOfBlocks;

    /** constructor */
    private FileINode(String name, double numOfBlocks) {
      super(name);
      this.numOfBlocks = numOfBlocks;
    }
    
    /** Output a file attribute */
    protected void outputFiles(PrintStream out, String prefix) {
      prefix = (prefix == null)?super.name: prefix + "/"+super.name;
      out.println(prefix + " " + numOfBlocks);
    }
  }

  private INode root;
  
  /** Generates a directory tree with a max depth of <code>maxDepth</code> */
  private void genDirStructure() {
    root = genDirStructure("", maxDepth);
  }
  
  /** Generate a directory tree rooted at <code>rootName</code>
   * The number of subtree is in the range of [minWidth, maxWidth].
   * The maximum depth of each subtree is in the range of
   * [2*maxDepth/3, maxDepth].
   */
  private INode genDirStructure(String rootName, int maxDepth) {
    INode root = new INode(rootName);
    
    if (maxDepth>0) {
      maxDepth--;
      int minDepth = maxDepth*2/3;
      // Figure out the number of subdirectories to generate
      int numOfSubDirs = minWidth + r.nextInt(maxWidth-minWidth+1);
      // Expand the tree
      for (int i=0; i<numOfSubDirs; i++) {
        int childDepth = (maxDepth == 0)?0:
          (r.nextInt(maxDepth-minDepth+1)+minDepth);
        INode child = genDirStructure("dir"+i, childDepth);
        root.addChild(child);
      }
    }
    return root;
  }
  
  /** Collects leaf nodes in the tree */
  private List<INode> getLeaves() {
    List<INode> leaveDirs = new ArrayList<INode>();
    root.getLeaves(leaveDirs);
    return leaveDirs;
  }
  
  /** Decides where to place all the files and its length.
   * It first collects all empty directories in the tree.
   * For each file, it randomly chooses an empty directory to place the file.
   * The file's length is generated using Gaussian distribution.
   */
  private void genFileStructure() {
    List<INode> leaves = getLeaves();
    int totalLeaves = leaves.size();
    for (int i=0; i<numOfFiles; i++) {
      int leaveNum = r.nextInt(totalLeaves);
      double fileSize;
      do {
        fileSize = r.nextGaussian()+avgFileSize;
      } while (fileSize<0);
      leaves.get(leaveNum).addChild(
          new FileINode(FILE_NAME_PREFIX+i, fileSize));
    }
  }
  
  /** Output directory structure to a file, each line of the file
   * contains the directory name. Only empty directory names are printed. */
  private void output(File outFile) throws FileNotFoundException {
    System.out.println("Printing to " + outFile.toString());
    PrintStream out = new PrintStream(outFile);
    root.output(out, null);
    out.close();
  }
  
  /** Output all files' attributes to a file, each line of the output file
   * contains a file name and its length. */
  private void outputFiles(File outFile) throws FileNotFoundException {
    System.out.println("Printing to " + outFile.toString());
    PrintStream out = new PrintStream(outFile);
    root.outputFiles(out, null);
    out.close();
  }
  
  /**
   * Main program
   * @param args Command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    StructureGenerator sg = new StructureGenerator();
    System.exit(sg.run(args));
  }
}
