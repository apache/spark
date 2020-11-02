/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.yarn;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.MergedShuffleFileManager;
import org.apache.spark.network.shuffle.RemoteBlockPushResolver;
import org.apache.spark.network.util.TransportConf;

public class YarnShuffleServiceSuite {

  @Test
  public void testCreateDefaultMergedShuffleFileManagerInstance() {
    TransportConf mockConf = mock(TransportConf.class);
    when(mockConf.mergedShuffleFileManagerImpl()).thenReturn(
      "org.apache.spark.network.shuffle.ExternalBlockHandler$NoOpMergedShuffleFileManager");
    MergedShuffleFileManager mergeMgr = YarnShuffleService.newMergedShuffleFileManagerInstance(
      mockConf);
    assertTrue(mergeMgr instanceof ExternalBlockHandler.NoOpMergedShuffleFileManager);
  }

  @Test
  public void testCreateRemoteBlockPushResolverInstance() {
    TransportConf mockConf = mock(TransportConf.class);
    when(mockConf.mergedShuffleFileManagerImpl()).thenReturn(
      "org.apache.spark.network.shuffle.RemoteBlockPushResolver");
    MergedShuffleFileManager mergeMgr = YarnShuffleService.newMergedShuffleFileManagerInstance(
      mockConf);
    assertTrue(mergeMgr instanceof RemoteBlockPushResolver);
  }

  @Test
  public void testInvalidClassNameOfMergeManagerWillUseNoOpInstance() {
    TransportConf mockConf = mock(TransportConf.class);
    when(mockConf.mergedShuffleFileManagerImpl()).thenReturn(
      "org.apache.spark.network.shuffle.NotExistent");
    MergedShuffleFileManager mergeMgr = YarnShuffleService.newMergedShuffleFileManagerInstance(
      mockConf);
    assertTrue(mergeMgr instanceof ExternalBlockHandler.NoOpMergedShuffleFileManager);
  }
}
