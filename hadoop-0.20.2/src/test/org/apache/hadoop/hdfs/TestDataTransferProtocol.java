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
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This tests data transfer protocol handling in the Datanode. It sends
 * various forms of wrong data and verifies that Datanode handles it well.
 */
public class TestDataTransferProtocol extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(
                    "org.apache.hadoop.hdfs.TestDataTransferProtocol");
  
  DatanodeID datanode;
  InetSocketAddress dnAddr;
  ByteArrayOutputStream sendBuf = new ByteArrayOutputStream(128);
  DataOutputStream sendOut = new DataOutputStream(sendBuf);
  // byte[] recvBuf = new byte[128];
  // ByteBuffer recvByteBuf = ByteBuffer.wrap(recvBuf);
  ByteArrayOutputStream recvBuf = new ByteArrayOutputStream(128);
  DataOutputStream recvOut = new DataOutputStream(recvBuf);

  private void sendRecvData(String testDescription,
                            boolean eofExpected) throws IOException {
    /* Opens a socket to datanode
     * sends the data in sendBuf.
     * If there is data in expectedBuf, expects to receive the data
     *     from datanode that matches expectedBuf.
     * If there is an exception while recieving, throws it
     *     only if exceptionExcepted is false.
     */
    
    Socket sock = null;
    try {
      
      if ( testDescription != null ) {
        LOG.info("Testing : " + testDescription);
      }
      sock = new Socket();
      sock.connect(dnAddr, HdfsConstants.READ_TIMEOUT);
      sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);
      
      OutputStream out = sock.getOutputStream();
      // Should we excuse 
      byte[] retBuf = new byte[recvBuf.size()];
      
      DataInputStream in = new DataInputStream(sock.getInputStream());
      out.write(sendBuf.toByteArray());
      try {
        in.readFully(retBuf);
      } catch (EOFException eof) {
        if ( eofExpected ) {
          LOG.info("Got EOF as expected.");
          return;
        }
        throw eof;
      }
      for (int i=0; i<retBuf.length; i++) {
        System.out.print(retBuf[i]);
      }
      System.out.println(":");
      
      if (eofExpected) {
        throw new IOException("Did not recieve IOException when an exception " +
                              "is expected while reading from " + 
                              datanode.getName());
      }
      
      byte[] needed = recvBuf.toByteArray();
      for (int i=0; i<retBuf.length; i++) {
        System.out.print(retBuf[i]);
        assertEquals("checking byte[" + i + "]", needed[i], retBuf[i]);
      }
    } finally {
      IOUtils.closeSocket(sock);
    }
  }
  
  void createFile(FileSystem fs, Path path, int fileLen) throws IOException {
    byte [] arr = new byte[fileLen];
    FSDataOutputStream out = fs.create(path);
    out.write(arr);
    out.close();
  }
  
  void readFile(FileSystem fs, Path path, int fileLen) throws IOException {
    byte [] arr = new byte[fileLen];
    FSDataInputStream in = fs.open(path);
    in.readFully(arr);
  }
  
  public void testDataTransferProtocol() throws IOException {
    Random random = new Random();
    int oneMil = 1024*1024;
    Path file = new Path("dataprotocol.dat");
    int numDataNodes = 1;
    
    Configuration conf = new Configuration();
    conf.setInt("dfs.replication", numDataNodes); 
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    cluster.waitActive();
    DFSClient dfsClient = new DFSClient(
                 new InetSocketAddress("localhost", cluster.getNameNodePort()),
                 conf);                
    datanode = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    dnAddr = NetUtils.createSocketAddr(datanode.getName());
    FileSystem fileSys = cluster.getFileSystem();
    
    int fileLen = Math.min(conf.getInt("dfs.block.size", 4096), 4096);
    
    createFile(fileSys, file, fileLen);

    // get the first blockid for the file
    Block firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
    long newBlockId = firstBlock.getBlockId() + 1;

    recvBuf.reset();
    sendBuf.reset();
    
    // bad version
    recvOut.writeShort((short)(DataTransferProtocol.DATA_TRANSFER_VERSION-1));
    sendOut.writeShort((short)(DataTransferProtocol.DATA_TRANSFER_VERSION-1));
    sendRecvData("Wrong Version", true);

    // bad ops
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)(DataTransferProtocol.OP_WRITE_BLOCK-1));
    sendRecvData("Wrong Op Code", true);
    
    /* Test OP_WRITE_BLOCK */
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_WRITE_BLOCK);
    sendOut.writeLong(newBlockId); // block id
    sendOut.writeLong(0);          // generation stamp
    sendOut.writeInt(0);           // targets in pipeline 
    sendOut.writeBoolean(false);   // recoveryFlag
    Text.writeString(sendOut, "cl");// clientID
    sendOut.writeBoolean(false); // no src node info
    sendOut.writeInt(0);           // number of downstream targets
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    
    // bad bytes per checksum
    sendOut.writeInt(-1-random.nextInt(oneMil));
    recvBuf.reset();
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR);
    sendRecvData("wrong bytesPerChecksum while writing", true);

    sendBuf.reset();
    recvBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_WRITE_BLOCK);
    sendOut.writeLong(newBlockId);
    sendOut.writeLong(0);          // generation stamp
    sendOut.writeInt(0);           // targets in pipeline 
    sendOut.writeBoolean(false);   // recoveryFlag
    Text.writeString(sendOut, "cl");// clientID
    sendOut.writeBoolean(false); // no src node info

    // bad number of targets
    sendOut.writeInt(-1-random.nextInt(oneMil));
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR);
    sendRecvData("bad targets len while writing block " + newBlockId, true);

    sendBuf.reset();
    recvBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_WRITE_BLOCK);
    sendOut.writeLong(++newBlockId);
    sendOut.writeLong(0);          // generation stamp
    sendOut.writeInt(0);           // targets in pipeline 
    sendOut.writeBoolean(false);   // recoveryFlag
    Text.writeString(sendOut, "cl");// clientID
    sendOut.writeBoolean(false); // no src node info
    sendOut.writeInt(0);
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    sendOut.writeInt((int)512);
    sendOut.writeInt(4);           // size of packet
    sendOut.writeLong(0);          // OffsetInBlock
    sendOut.writeLong(100);        // sequencenumber
    sendOut.writeBoolean(false);   // lastPacketInBlock
    
    // bad data chunk length
    sendOut.writeInt(-1-random.nextInt(oneMil));
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_SUCCESS);
    Text.writeString(recvOut, ""); // first bad node
    recvOut.writeLong(100);        // sequencenumber
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR);
    sendRecvData("negative DATA_CHUNK len while writing block " + newBlockId, 
                 true);

    // test for writing a valid zero size block
    sendBuf.reset();
    recvBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_WRITE_BLOCK);
    sendOut.writeLong(++newBlockId);
    sendOut.writeLong(0);          // generation stamp
    sendOut.writeInt(0);           // targets in pipeline 
    sendOut.writeBoolean(false);   // recoveryFlag
    Text.writeString(sendOut, "cl");// clientID
    sendOut.writeBoolean(false); // no src node info
    sendOut.writeInt(0);
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    sendOut.writeInt((int)512);    // checksum size
    sendOut.writeInt(8);           // size of packet
    sendOut.writeLong(0);          // OffsetInBlock
    sendOut.writeLong(100);        // sequencenumber
    sendOut.writeBoolean(true);    // lastPacketInBlock

    sendOut.writeInt(0);           // chunk length
    sendOut.writeInt(0);           // zero checksum
    //ok finally write a block with 0 len
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_SUCCESS);
    Text.writeString(recvOut, ""); // first bad node
    new DataTransferProtocol.PipelineAck(100, 
        new short[]{DataTransferProtocol.OP_STATUS_SUCCESS}).write(recvOut);
    sendRecvData("Writing a zero len block blockid " + newBlockId, false);
    
    /* Test OP_READ_BLOCK */

    // bad block id
    sendBuf.reset();
    recvBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_READ_BLOCK);
    newBlockId = firstBlock.getBlockId()-1;
    sendOut.writeLong(newBlockId);
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0L);
    sendOut.writeLong(fileLen);
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR);
    Text.writeString(sendOut, "cl");
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Wrong block ID " + newBlockId + " for read", false); 

    // negative block start offset
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_READ_BLOCK);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(-1L);
    sendOut.writeLong(fileLen);
    Text.writeString(sendOut, "cl");
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Negative start-offset for read for block " + 
                 firstBlock.getBlockId(), false);

    // bad block start offset
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_READ_BLOCK);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(fileLen);
    sendOut.writeLong(fileLen);
    Text.writeString(sendOut, "cl");
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Wrong start-offset for reading block " +
                 firstBlock.getBlockId(), false);
    
    // negative length is ok. Datanode assumes we want to read the whole block.
    recvBuf.reset();
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_SUCCESS);    
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_READ_BLOCK);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0);
    sendOut.writeLong(-1-random.nextInt(oneMil));
    Text.writeString(sendOut, "cl");
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Negative length for reading block " +
                 firstBlock.getBlockId(), false);
    
    // length is more than size of block.
    recvBuf.reset();
    recvOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR);    
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_READ_BLOCK);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0);
    sendOut.writeLong(fileLen + 1);
    Text.writeString(sendOut, "cl");
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Wrong length for reading block " +
                 firstBlock.getBlockId(), false);
    
    //At the end of all this, read the file to make sure that succeeds finally.
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte((byte)DataTransferProtocol.OP_READ_BLOCK);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0);
    sendOut.writeLong(fileLen);
    Text.writeString(sendOut, "cl");
    BlockTokenSecretManager.DUMMY_TOKEN.write(sendOut);
    readFile(fileSys, file, fileLen);
  }
}
