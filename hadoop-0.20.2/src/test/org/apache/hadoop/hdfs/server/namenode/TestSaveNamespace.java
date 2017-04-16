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

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test various failure scenarios during saveNamespace() operation.
 * Cases covered:
 * <ol>
 * <li>Recover from failure while saving into the second storage directory</li>
 * <li>Recover from failure while moving current into lastcheckpoint.tmp</li>
 * <li>Recover from failure while moving lastcheckpoint.tmp into
 * previous.checkpoint</li>
 * <li>Recover from failure while rolling edits file</li>
 * </ol>
 */
public class TestSaveNamespace extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestSaveNamespace.class);

  private static class FaultySaveImage implements Answer<Void> {
    int count = 0;
    FSImage origImage;

    public FaultySaveImage(FSImage origImage) {
      this.origImage = origImage;
    }

    public Void answer(InvocationOnMock invocation) throws Exception {
      Object[] args = invocation.getArguments();
      File f = (File)args[0];

      if (count++ == 1) {
        LOG.info("Injecting fault for file: " + f);
        throw new RuntimeException("Injected fault: saveFSImage second time");
      }
      LOG.info("Not injecting fault for file: " + f);
      origImage.saveFSImage(f);
      return null;
    }
  }

  private enum Fault {
    SAVE_FSIMAGE,
    MOVE_CURRENT,
    MOVE_LAST_CHECKPOINT
  };

  private void saveNamespaceWithInjectedFault(Fault fault) throws IOException {
    Configuration conf = getConf();
    NameNode.myMetrics = new NameNodeMetrics(conf, null);
    NameNode.format(conf);
    NameNode nn = new NameNode(conf);
    FSNamesystem fsn = nn.getNamesystem();

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;

    // inject fault
    switch(fault) {
    case SAVE_FSIMAGE:
      // The spy throws a RuntimeException when writing to the second directory
      doAnswer(new FaultySaveImage(originalImage)).
        when(spyImage).saveFSImage((File)anyObject());
      break;
    case MOVE_CURRENT:
      // The spy throws a RuntimeException when calling moveCurrent()
      doThrow(new RuntimeException("Injected fault: moveCurrent")).
        when(spyImage).moveCurrent((StorageDirectory)anyObject());
      break;
    case MOVE_LAST_CHECKPOINT:
      // The spy throws a RuntimeException when calling moveLastCheckpoint()
      doThrow(new RuntimeException("Injected fault: moveLastCheckpoint")).
        when(spyImage).moveLastCheckpoint((StorageDirectory)anyObject());
      break;
    }

    try {
      doAnEdit(fsn, 1);

      // Save namespace - this will fail because we inject a fault.
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        fsn.saveNamespace();
      } catch (Exception e) {
        LOG.info("Test caught expected exception", e);
      }

      // Now shut down and restart the namesystem
      nn.stop();
      nn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      nn = new NameNode(conf);
      fsn = nn.getNamesystem();

      // Make sure the image loaded including our edit.
      checkEditExists(fsn, 1);
    } finally {
      if (nn != null) {
        nn.stop();
      }
    }
  }

  // @Test
  public void testCrashWhileSavingSecondImage() throws Exception {
    saveNamespaceWithInjectedFault(Fault.SAVE_FSIMAGE);
  }

  // @Test
  public void testCrashWhileMoveCurrent() throws Exception {
    saveNamespaceWithInjectedFault(Fault.MOVE_CURRENT);
  }

  // @Test
  public void testCrashWhileMoveLastCheckpoint() throws Exception {
    saveNamespaceWithInjectedFault(Fault.MOVE_LAST_CHECKPOINT);
  }

  // @Test
  public void testSaveWhileEditsRolled() throws Exception {
    Configuration conf = getConf();
    NameNode.myMetrics = new NameNodeMetrics(conf, null);
    NameNode.format(conf);
    NameNode nn = new NameNode(conf);
    FSNamesystem fsn = nn.getNamesystem();

    // Replace the FSImage with a spy
    final FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;

    try {
      doAnEdit(fsn, 1);
      CheckpointSignature sig = fsn.rollEditLog();
      LOG.warn("Checkpoint signature: " + sig);
      // Do another edit
      doAnEdit(fsn, 2);

      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fsn.saveNamespace();

      // Now shut down and restart the NN
      nn.stop();
      nn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      nn = new NameNode(conf);
      fsn = nn.getNamesystem();

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
      checkEditExists(fsn, 2);
    } finally {
      if (nn != null) {
        nn.stop();
      }
    }
  }

  private void doAnEdit(FSNamesystem fsn, int id) throws IOException {
    // Make an edit
    fsn.mkdirs(
      "/test" + id,
      new PermissionStatus("test", "Test",
          new FsPermission((short)0777)));
  }

  private void checkEditExists(FSNamesystem fsn, int id) throws IOException {
    // Make sure the image loaded including our edit.
    assertNotNull(fsn.getFileInfo("/test" + id));
  }

  private Configuration getConf() throws IOException {
    String baseDir = System.getProperty("test.build.data", "build/test/data/dfs/");
    String nameDirs = baseDir + "name1" + "," + baseDir + "name2";
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set("dfs.http.address", "0.0.0.0:0");
    conf.set("dfs.name.dir", nameDirs);
    conf.set("dfs.name.edits.dir", nameDirs);
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    conf.setBoolean("dfs.permissions", false); 
    return conf;
  }
}
