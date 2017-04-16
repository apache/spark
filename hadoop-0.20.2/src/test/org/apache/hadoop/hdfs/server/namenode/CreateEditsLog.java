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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

/**
 * 
 * CreateEditsLog
 *   Synopsis: CreateEditsLog -f numFiles StartingBlockId numBlocksPerFile
 *        [-r replicafactor] [-d editsLogDirectory]
 *             Default replication factor is 1
 *             Default edits log directory is /tmp/EditsLogOut
 *   
 *   Create a name node's edits log in /tmp/EditsLogOut.
 *   The file /tmp/EditsLogOut/current/edits can be copied to a name node's
 *   dfs.name.dir/current direcotry and the name node can be started as usual.
 *   
 *   The files are created in /createdViaInjectingInEditsLog
 *   The file names contain the starting and ending blockIds; hence once can 
 *   create multiple edits logs using this command using non overlapping 
 *   block ids and feed the files to a single name node.
 *   
 *   See Also @link #DataNodeCluster for injecting a set of matching
 *   blocks created with this command into a set of simulated data nodes.
 *
 */

public class CreateEditsLog {
  static final String BASE_PATH = "/createdViaInjectingInEditsLog";
  static final String EDITS_DIR = "/tmp/EditsLogOut";
  static String edits_dir = EDITS_DIR;
  static final public long BLOCK_GENERATION_STAMP =
    GenerationStamp.FIRST_VALID_STAMP;
  
  static void addFiles(FSEditLog editLog, int numFiles, short replication, 
                         int blocksPerFile, long startingBlockId,
                         FileNameGenerator nameGenerator) {
    
    PermissionStatus p = new PermissionStatus("joeDoe", "people",
                                      new FsPermission((short)0777));
    INodeDirectory dirInode = new INodeDirectory(p, 0L);
    editLog.logMkDir(BASE_PATH, dirInode);
    long blockSize = 10;
    BlockInfo[] blocks = new BlockInfo[blocksPerFile];
    for (int iB = 0; iB < blocksPerFile; ++iB) {
      blocks[iB] = 
       new BlockInfo(new Block(0, blockSize, BLOCK_GENERATION_STAMP),
                               replication);
    }
    
    long currentBlockId = startingBlockId;
    long bidAtSync = startingBlockId;

    for (int iF = 0; iF < numFiles; iF++) {
      for (int iB = 0; iB < blocksPerFile; ++iB) {
         blocks[iB].setBlockId(currentBlockId++);
      }

      try {

        INodeFileUnderConstruction inode = new INodeFileUnderConstruction(
                      null, replication, 0, blockSize, blocks, p, "", "", null);
        // Append path to filename with information about blockIDs 
        String path = "_" + iF + "_B" + blocks[0].getBlockId() + 
                      "_to_B" + blocks[blocksPerFile-1].getBlockId() + "_";
        String filePath = nameGenerator.getNextFileName("");
        filePath = filePath + path;
        // Log the new sub directory in edits
        if ((iF % nameGenerator.getFilesPerDirectory())  == 0) {
          String currentDir = nameGenerator.getCurrentDir();
          dirInode = new INodeDirectory(p, 0L);
          editLog.logMkDir(currentDir, dirInode);
        }
        editLog.logOpenFile(filePath, inode);
        editLog.logCloseFile(filePath, inode);

        if (currentBlockId - bidAtSync >= 2000) { // sync every 2K blocks
          editLog.logSync();
          bidAtSync = currentBlockId;
        }
      } catch (IOException e) {
        System.out.println("Creating trascation for file " + iF +
            " encountered exception " + e);
      }
    }
    System.out.println("Created edits log in directory " + edits_dir);
    System.out.println(" containing " +
       numFiles + " File-Creates, each file with " + blocksPerFile + " blocks");
    System.out.println(" blocks range: " + 
        startingBlockId + " to " + (currentBlockId-1));
  }
  
  static String usage = "Usage: createditlogs " +
  " -f  numFiles startingBlockIds NumBlocksPerFile  [-r replicafactor] " + 
  		"[-d editsLogDirectory]\n" + 
  		"      Default replication factor is 1\n" +
  		"      Default edits log direcory is " + EDITS_DIR + "\n";



  static void printUsageExit() {
    System.out.println(usage);
    System.exit(-1); 
    }
    static void printUsageExit(String err) {
    System.out.println(err);
    printUsageExit();
  }
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {



    long startingBlockId = 1;
    int numFiles = 0;
    short replication = 1;
    int numBlocksPerFile = 0;

    if (args.length == 0) {
      printUsageExit();
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-h"))
        printUsageExit();
      if (args[i].equals("-f")) {
       if (i + 3 >= args.length || args[i+1].startsWith("-") || 
           args[i+2].startsWith("-") || args[i+3].startsWith("-")) {
         printUsageExit(
             "Missing num files, starting block and/or number of blocks");
       }
       numFiles = Integer.parseInt(args[++i]);
       startingBlockId = Integer.parseInt(args[++i]);
       numBlocksPerFile = Integer.parseInt(args[++i]);
       if (numFiles <=0 || numBlocksPerFile <= 0) {
         printUsageExit("numFiles and numBlocksPerFile most be greater than 0");
       }
      } else if (args[i].equals("-r") || args[i+1].startsWith("-")) {
        if (i + 1 >= args.length) {
          printUsageExit(
              "Missing num files, starting block and/or number of blocks");
        }
        replication = Short.parseShort(args[++i]);
      } else if (args[i].equals("-d")) {
        if (i + 1 >= args.length || args[i+1].startsWith("-")) {
          printUsageExit("Missing edits logs directory");
        }
        edits_dir = args[++i];
      } else {
        printUsageExit();
      }
    }
    

    File editsLogDir = new File(edits_dir);
    File subStructureDir = new File(edits_dir + "/" + 
        Storage.STORAGE_DIR_CURRENT);
    if ( !editsLogDir.exists() ) {
      if ( !editsLogDir.mkdir()) {
        System.out.println("cannot create " + edits_dir);
        System.exit(-1);
      }
    }
    if ( !subStructureDir.exists() ) {
      if ( !subStructureDir.mkdir()) {
        System.out.println("cannot create subdirs of " + edits_dir);
        System.exit(-1);
      }
    }
  
    FSImage fsImage = new FSImage(new File(edits_dir));
    FileNameGenerator nameGenerator = new FileNameGenerator(BASE_PATH, 100);


    FSEditLog editLog = fsImage.getEditLog();
    editLog.createEditLogFile(fsImage.getFsEditName());
    editLog.open();
    addFiles(editLog, numFiles, replication, numBlocksPerFile, startingBlockId,
             nameGenerator);
    editLog.logSync();
    editLog.close();
  }
}
