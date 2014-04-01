package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;

import junit.framework.TestCase;

public class TestUnderReplicatedBlocks extends TestCase {
  public void testSetrepIncWithUnderReplicatedBlocks() throws Exception {
    Configuration conf = new Configuration();
    final short REPLICATION_FACTOR = 2;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, REPLICATION_FACTOR+1, true, null);
    try {
      // create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      
      // remove one replica from the blocksMap so block becomes under-replicated
      // but the block does not get put into the under-replicated blocks queue
      FSNamesystem namesystem = cluster.getNameNode().namesystem;
      Block b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      DatanodeDescriptor dn = namesystem.blocksMap.nodeIterator(b).next();
      namesystem.addToInvalidates(b, dn);
      namesystem.blocksMap.removeNode(b, dn);
      
      // increment this file's replication factor
      FsShell shell = new FsShell(conf);
      assertEquals(0, shell.run(new String[]{
          "-setrep", "-w", Integer.toString(1+REPLICATION_FACTOR), FILE_NAME}));
    } finally {
      cluster.shutdown();
    }
    
  }

}
