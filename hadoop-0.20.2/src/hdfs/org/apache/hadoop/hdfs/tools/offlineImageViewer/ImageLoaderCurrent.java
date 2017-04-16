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

import java.io.DataInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor.ImageElement;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/**
 * ImageLoaderCurrent processes Hadoop FSImage files and walks over
 * them using a provided ImageVisitor, calling the visitor at each element
 * enumerated below.
 *
 * The only difference between v18 and v19 was the utilization of the
 * stickybit.  Therefore, the same viewer can reader either format.
 *
 * Versions -19 fsimage layout (with changes from -16 up):
 * Image version (int)
 * Namepsace ID (int)
 * NumFiles (long)
 * Generation stamp (long)
 * INodes (count = NumFiles)
 *  INode
 *    Path (String)
 *    Replication (short)
 *    Modification Time (long as date)
 *    Access Time (long) // added in -16
 *    Block size (long)
 *    Num blocks (int)
 *    Blocks (count = Num blocks)
 *      Block
 *        Block ID (long)
 *        Num bytes (long)
 *        Generation stamp (long)
 *    Namespace Quota (long)
 *    Diskspace Quota (long) // added in -18
 *    Permissions
 *      Username (String)
 *      Groupname (String)
 *      OctalPerms (short -> String)  // Modified in -19
 * NumINodesUnderConstruction (int)
 * INodesUnderConstruction (count = NumINodesUnderConstruction)
 *  INodeUnderConstruction
 *    Path (bytes as string)
 *    Replication (short)
 *    Modification time (long as date)
 *    Preferred block size (long)
 *    Num blocks (int)
 *    Blocks
 *      Block
 *        Block ID (long)
 *        Num bytes (long)
 *        Generation stamp (long)
 *    Permissions
 *      Username (String)
 *      Groupname (String)
 *      OctalPerms (short -> String)
 *    Client Name (String)
 *    Client Machine (String)
 *    NumLocations (int)
 *    DatanodeDescriptors (count = numLocations) // not loaded into memory
 *      short                                    // but still in file
 *      long
 *      string
 *      long
 *      int
 *      string
 *      string
 *      enum
 *
 */
class ImageLoaderCurrent implements ImageLoader {
  protected final DateFormat dateFormat = 
                                      new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static int [] versions = {-16, -17, -18, -19};
  private int imageVersion = 0;

  /* (non-Javadoc)
   * @see ImageLoader#canProcessVersion(int)
   */
  @Override
  public boolean canLoadVersion(int version) {
    for(int v : versions)
      if(v == version) return true;

    return false;
  }

  /* (non-Javadoc)
   * @see ImageLoader#processImage(java.io.DataInputStream, ImageVisitor, boolean)
   */
  @Override
  public void loadImage(DataInputStream in, ImageVisitor v,
      boolean skipBlocks) throws IOException {
    try {
      v.start();
      v.visitEnclosingElement(ImageElement.FS_IMAGE);

      imageVersion = in.readInt();
      if( !canLoadVersion(imageVersion))
        throw new IOException("Cannot process fslayout version " + imageVersion);

      v.visit(ImageElement.IMAGE_VERSION, imageVersion);
      v.visit(ImageElement.NAMESPACE_ID, in.readInt());

      long numInodes = in.readLong();

      v.visit(ImageElement.GENERATION_STAMP, in.readLong());

      processINodes(in, v, numInodes, skipBlocks);

      processINodesUC(in, v, skipBlocks);

      v.leaveEnclosingElement(); // FSImage
      v.finish();
    } catch(IOException e) {
      // Tell the visitor to clean up, then re-throw the exception
      v.finishAbnormally();
      throw e;
    }
  }

  /**
   * Process the INodes under construction section of the fsimage.
   *
   * @param in DataInputStream to process
   * @param v Visitor to walk over inodes
   * @param skipBlocks Walk over each block?
   */
  private void processINodesUC(DataInputStream in, ImageVisitor v,
      boolean skipBlocks) throws IOException {
    int numINUC = in.readInt();

    v.visitEnclosingElement(ImageElement.INODES_UNDER_CONSTRUCTION,
                           ImageElement.NUM_INODES_UNDER_CONSTRUCTION, numINUC);

    for(int i = 0; i < numINUC; i++) {
      v.visitEnclosingElement(ImageElement.INODE_UNDER_CONSTRUCTION);
      byte [] name = FSImage.readBytes(in);
      String n = new String(name, "UTF8");
      v.visit(ImageElement.INODE_PATH, n);
      v.visit(ImageElement.REPLICATION, in.readShort());
      v.visit(ImageElement.MODIFICATION_TIME, formatDate(in.readLong()));

      v.visit(ImageElement.PREFERRED_BLOCK_SIZE, in.readLong());
      int numBlocks = in.readInt();
      processBlocks(in, v, numBlocks, skipBlocks);

      processPermission(in, v);
      v.visit(ImageElement.CLIENT_NAME, FSImage.readString(in));
      v.visit(ImageElement.CLIENT_MACHINE, FSImage.readString(in));

      // Skip over the datanode descriptors, which are still stored in the
      // file but are not used by the datanode or loaded into memory
      int numLocs = in.readInt();
      for(int j = 0; j < numLocs; j++) {
        in.readShort();
        in.readLong();
        in.readLong();
        in.readLong();
        in.readInt();
        FSImage.readString(in);
        FSImage.readString(in);
        WritableUtils.readEnum(in, AdminStates.class);
      }

      v.leaveEnclosingElement(); // INodeUnderConstruction
    }

    v.leaveEnclosingElement(); // INodesUnderConstruction
  }

  /**
   * Process the blocks section of the fsimage.
   *
   * @param in Datastream to process
   * @param v Visitor to walk over inodes
   * @param skipBlocks Walk over each block?
   */
  private void processBlocks(DataInputStream in, ImageVisitor v,
      int numBlocks, boolean skipBlocks) throws IOException {
    v.visitEnclosingElement(ImageElement.BLOCKS,
                            ImageElement.NUM_BLOCKS, numBlocks);
    
    if(numBlocks == -1) { // directory, no blocks to process
      v.leaveEnclosingElement(); // Blocks
      return;
    }
    
    if(skipBlocks) {
      int bytesToSkip = ((Long.SIZE * 3 /* fields */) / 8 /*bits*/) * numBlocks;
      if(in.skipBytes(bytesToSkip) != bytesToSkip)
        throw new IOException("Error skipping over blocks");
      
    } else {
      for(int j = 0; j < numBlocks; j++) {
        v.visitEnclosingElement(ImageElement.BLOCK);
        v.visit(ImageElement.BLOCK_ID, in.readLong());
        v.visit(ImageElement.NUM_BYTES, in.readLong());
        v.visit(ImageElement.GENERATION_STAMP, in.readLong());
        v.leaveEnclosingElement(); // Block
      }
    }
    v.leaveEnclosingElement(); // Blocks
  }

  /**
   * Extract the INode permissions stored in the fsimage file.
   *
   * @param in Datastream to process
   * @param v Visitor to walk over inodes
   */
  private void processPermission(DataInputStream in, ImageVisitor v)
      throws IOException {
    v.visitEnclosingElement(ImageElement.PERMISSIONS);
    v.visit(ImageElement.USER_NAME, Text.readString(in));
    v.visit(ImageElement.GROUP_NAME, Text.readString(in));
    FsPermission fsp = new FsPermission(in.readShort());
    v.visit(ImageElement.PERMISSION_STRING, fsp.toString());
    v.leaveEnclosingElement(); // Permissions
  }

  /**
   * Process the INode records stored in the fsimage.
   *
   * @param in Datastream to process
   * @param v Visitor to walk over INodes
   * @param numInodes Number of INodes stored in file
   * @param skipBlocks Process all the blocks within the INode?
   * @throws VisitException
   * @throws IOException
   */
  private void processINodes(DataInputStream in, ImageVisitor v,
      long numInodes, boolean skipBlocks) throws IOException {
    v.visitEnclosingElement(ImageElement.INODES,
        ImageElement.NUM_INODES, numInodes);

    for(long i = 0; i < numInodes; i++) {
      v.visitEnclosingElement(ImageElement.INODE);
      v.visit(ImageElement.INODE_PATH, FSImage.readString(in));
      v.visit(ImageElement.REPLICATION, in.readShort());
      v.visit(ImageElement.MODIFICATION_TIME, formatDate(in.readLong()));
      if(imageVersion <= -17) // added in version -17
        v.visit(ImageElement.ACCESS_TIME, formatDate(in.readLong()));
      v.visit(ImageElement.BLOCK_SIZE, in.readLong());
      int numBlocks = in.readInt();

      processBlocks(in, v, numBlocks, skipBlocks);

      if(numBlocks != 0) {
        v.visit(ImageElement.NS_QUOTA, numBlocks <= 0 ? in.readLong() : -1);
        if(imageVersion <= -18) // added in version -18
          v.visit(ImageElement.DS_QUOTA, numBlocks <= 0 ? in.readLong() : -1);
      }

      processPermission(in, v);
      v.leaveEnclosingElement(); // INode
    }
    
    v.leaveEnclosingElement(); // INodes
  }

  /**
   * Helper method to format dates during processing.
   * @param date Date as read from image file
   * @return String version of date format
   */
  private String formatDate(long date) {
    return dateFormat.format(new Date(date));
  }
}
