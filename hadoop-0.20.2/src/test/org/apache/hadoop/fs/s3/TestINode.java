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

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.hadoop.fs.s3.INode.FileType;

public class TestINode extends TestCase {

  public void testSerializeFileWithSingleBlock() throws IOException {
    Block[] blocks = { new Block(849282477840258181L, 128L) };
    INode inode = new INode(FileType.FILE, blocks);

    assertEquals("Length", 1L + 4 + 16, inode.getSerializedLength());
    InputStream in = inode.serialize();

    INode deserialized = INode.deserialize(in);

    assertEquals("FileType", inode.getFileType(), deserialized.getFileType());
    Block[] deserializedBlocks = deserialized.getBlocks();
    assertEquals("Length", 1, deserializedBlocks.length);
    assertEquals("Id", blocks[0].getId(), deserializedBlocks[0].getId());
    assertEquals("Length", blocks[0].getLength(), deserializedBlocks[0]
                 .getLength());

  }
  
  public void testSerializeDirectory() throws IOException {
    INode inode = INode.DIRECTORY_INODE;
    assertEquals("Length", 1L, inode.getSerializedLength());
    InputStream in = inode.serialize();
    INode deserialized = INode.deserialize(in);    
    assertSame(INode.DIRECTORY_INODE, deserialized);
  }
  
  public void testDeserializeNull() throws IOException {
    assertNull(INode.deserialize(null));
  }

}
