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

package org.apache.hadoop.fs.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Holds file metadata including type (regular file, or directory),
 * and the list of blocks that are pointers to the data.
 */
public class INode {
	
  enum FileType {
    DIRECTORY, FILE
  }
  
  public static final FileType[] FILE_TYPES = {
    FileType.DIRECTORY,
    FileType.FILE
  };

  public static final INode DIRECTORY_INODE = new INode(FileType.DIRECTORY, null);
  
  private FileType fileType;
  private Block[] blocks;

  public INode(FileType fileType, Block[] blocks) {
    this.fileType = fileType;
    if (isDirectory() && blocks != null) {
      throw new IllegalArgumentException("A directory cannot contain blocks.");
    }
    this.blocks = blocks;
  }

  public Block[] getBlocks() {
    return blocks;
  }
  
  public FileType getFileType() {
    return fileType;
  }

  public boolean isDirectory() {
    return fileType == FileType.DIRECTORY;
  }  

  public boolean isFile() {
    return fileType == FileType.FILE;
  }
  
  public long getSerializedLength() {
    return 1L + (blocks == null ? 0 : 4 + blocks.length * 16);
  }
  

  public InputStream serialize() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(fileType.ordinal());
    if (isFile()) {
      out.writeInt(blocks.length);
      for (int i = 0; i < blocks.length; i++) {
        out.writeLong(blocks[i].getId());
        out.writeLong(blocks[i].getLength());
      }
    }
    out.close();
    return new ByteArrayInputStream(bytes.toByteArray());
  }
  
  public static INode deserialize(InputStream in) throws IOException {
    if (in == null) {
      return null;
    }
    DataInputStream dataIn = new DataInputStream(in);
    FileType fileType = INode.FILE_TYPES[dataIn.readByte()];
    switch (fileType) {
    case DIRECTORY:
      in.close();
      return INode.DIRECTORY_INODE;
    case FILE:
      int numBlocks = dataIn.readInt();
      Block[] blocks = new Block[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        long id = dataIn.readLong();
        long length = dataIn.readLong();
        blocks[i] = new Block(id, length);
      }
      in.close();
      return new INode(fileType, blocks);
    default:
      throw new IllegalArgumentException("Cannot deserialize inode.");
    }    
  }  
  
}
