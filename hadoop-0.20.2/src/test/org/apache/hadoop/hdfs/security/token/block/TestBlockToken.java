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

package org.apache.hadoop.hdfs.security.token.block;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.TestWritable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Unit tests for block tokens */
public class TestBlockToken {
  public static final Log LOG = LogFactory.getLog(TestBlockToken.class);
  private static final String ADDRESS = "0.0.0.0";

  static final String SERVER_PRINCIPAL_KEY = "test.ipc.server.principal";
  private static Configuration conf;
  static {
    conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  static {
    ((Log4JLogger) Client.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) Server.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslInputStream.LOG).getLogger().setLevel(Level.ALL);
  }

  long blockKeyUpdateInterval = 10 * 60 * 1000; // 10 mins
  long blockTokenLifetime = 2 * 60 * 1000; // 2 mins
  Block block1 = new Block(0L);
  Block block2 = new Block(10L);
  Block block3 = new Block(-108L);

  private static class getLengthAnswer implements Answer<LocatedBlock> {
    BlockTokenSecretManager sm;
    BlockTokenIdentifier ident;

    public getLengthAnswer(BlockTokenSecretManager sm,
        BlockTokenIdentifier ident) {
      this.sm = sm;
      this.ident = ident;
    }

    @Override
    public LocatedBlock answer(InvocationOnMock invocation) throws IOException {
      Object args[] = invocation.getArguments();
      assertEquals(3, args.length);
      Block block = (Block) args[0];
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      assertEquals("Only one BlockTokenIdentifier expected", 1, tokenIds.size());
      LocatedBlock result = null;
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        LOG.info("Got: " + id.toString());
        assertTrue("Received BlockTokenIdentifier is wrong", ident.equals(id));
        sm.checkAccess(id, null, block, BlockTokenSecretManager.AccessMode.WRITE);
        result = new LocatedBlock(new Block(id.getBlockId()), null);
      }
      return result;
    }
  }

  private BlockTokenIdentifier generateTokenId(BlockTokenSecretManager sm,
      Block block, EnumSet<BlockTokenSecretManager.AccessMode> accessModes)
      throws IOException {
    Token<BlockTokenIdentifier> token = sm.generateToken(block, accessModes);
    BlockTokenIdentifier id = sm.createIdentifier();
    id.readFields(new DataInputStream(new ByteArrayInputStream(token
        .getIdentifier())));
    return id;
  }

  @Test
  public void testWritable() throws Exception {
    TestWritable.testWritable(new BlockTokenIdentifier());
    BlockTokenSecretManager sm = new BlockTokenSecretManager(true,
        blockKeyUpdateInterval, blockTokenLifetime);
    TestWritable.testWritable(generateTokenId(sm, block1, EnumSet
        .allOf(BlockTokenSecretManager.AccessMode.class)));
    TestWritable.testWritable(generateTokenId(sm, block2, EnumSet
        .of(BlockTokenSecretManager.AccessMode.WRITE)));
    TestWritable.testWritable(generateTokenId(sm, block3, EnumSet
        .noneOf(BlockTokenSecretManager.AccessMode.class)));
  }

  private void tokenGenerationAndVerification(BlockTokenSecretManager master,
      BlockTokenSecretManager slave) throws Exception {
    // single-mode tokens
    for (BlockTokenSecretManager.AccessMode mode : BlockTokenSecretManager.AccessMode
        .values()) {
      // generated by master
      Token<BlockTokenIdentifier> token1 = master.generateToken(block1,
          EnumSet.of(mode));
      master.checkAccess(token1, null, block1, mode);
      slave.checkAccess(token1, null, block1, mode);
      // generated by slave
      Token<BlockTokenIdentifier> token2 = slave.generateToken(block2,
          EnumSet.of(mode));
      master.checkAccess(token2, null, block2, mode);
      slave.checkAccess(token2, null, block2, mode);
    }
    // multi-mode tokens
    Token<BlockTokenIdentifier> mtoken = master.generateToken(block3, EnumSet
        .allOf(BlockTokenSecretManager.AccessMode.class));
    for (BlockTokenSecretManager.AccessMode mode : BlockTokenSecretManager.AccessMode
        .values()) {
      master.checkAccess(mtoken, null, block3, mode);
      slave.checkAccess(mtoken, null, block3, mode);
    }
  }

  /** test block key and token handling */
  @Test
  public void testBlockTokenSecretManager() throws Exception {
    BlockTokenSecretManager masterHandler = new BlockTokenSecretManager(true,
        blockKeyUpdateInterval, blockTokenLifetime);
    BlockTokenSecretManager slaveHandler = new BlockTokenSecretManager(false,
        blockKeyUpdateInterval, blockTokenLifetime);
    ExportedBlockKeys keys = masterHandler.exportKeys();
    slaveHandler.setKeys(keys);
    tokenGenerationAndVerification(masterHandler, slaveHandler);
    // key updating
    masterHandler.updateKeys();
    tokenGenerationAndVerification(masterHandler, slaveHandler);
    keys = masterHandler.exportKeys();
    slaveHandler.setKeys(keys);
    tokenGenerationAndVerification(masterHandler, slaveHandler);
  }

  @Test
  public void testBlockTokenRpc() throws Exception {
    BlockTokenSecretManager sm = new BlockTokenSecretManager(true,
        blockKeyUpdateInterval, blockTokenLifetime);
    Token<BlockTokenIdentifier> token = sm.generateToken(block3,
        EnumSet.allOf(BlockTokenSecretManager.AccessMode.class));

    ClientDatanodeProtocol mockDN = mock(ClientDatanodeProtocol.class);
    when(mockDN.getProtocolVersion(anyString(), anyLong())).thenReturn(
        ClientDatanodeProtocol.versionID);
    BlockTokenIdentifier id = sm.createIdentifier();
    id.readFields(new DataInputStream(new ByteArrayInputStream(token
        .getIdentifier())));
    doAnswer(new getLengthAnswer(sm, id)).when(mockDN).recoverBlock(
        any(Block.class), anyBoolean(), any(DatanodeInfo[].class));

    final Server server = RPC.getServer(mockDN,
        ADDRESS, 0, 5, true, conf, sm);

    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    final UserGroupInformation ticket = UserGroupInformation
        .createRemoteUser(block3.toString());
    ticket.addToken(token);

    ClientDatanodeProtocol proxy = null;
    try {
      proxy = (ClientDatanodeProtocol) RPC.getProxy(
          ClientDatanodeProtocol.class, ClientDatanodeProtocol.versionID, addr,
          ticket, conf, NetUtils.getDefaultSocketFactory(conf));
      LocatedBlock lb = proxy.recoverBlock(block3, true, null);
      assertEquals(block3.getBlockId(), lb.getBlock().getBlockId());
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
}
