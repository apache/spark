/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.handlers;

import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerDetailCollection;
import io.netty.channel.ChannelHandlerContext;
import org.apache.spark.remoteshuffle.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/***
 * This class handles messages as a registry server, which stores available shuffle server information.
 */
public class RegistryServerHandler {
  private static final Logger logger = LoggerFactory.getLogger(RegistryServerHandler.class);

  private final ServerDetailCollection serverCollection;

  private final String serverId;

  public RegistryServerHandler(ServerDetailCollection serverCollection, String serverId) {
    this.serverCollection = serverCollection;
    this.serverId = serverId;
  }

  public void handleMessage(ChannelHandlerContext ctx, ConnectRegistryRequest msg) {
    logger.debug("Handle message: " + msg);

    ConnectRegistryResponse response = new ConnectRegistryResponse(serverId);
    HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true);
  }

  public void handleMessage(ChannelHandlerContext ctx, RegisterServerRequestMessage request) {
    logger.info("Handling request: " + request);

    serverCollection.addServer(request.getDataCenter(), request.getCluster(),
        new ServerDetail(request.getServerId(), request.getConnectionString()));

    RegisterServerResponseMessage response =
        new RegisterServerResponseMessage(request.getServerId());
    HandlerUtil.writeResponseMsg(ctx, response);
  }

  public void handleMessage(ChannelHandlerContext ctx, GetServersRequestMessage request) {
    logger.info("Handling request: " + request);

    List<ServerDetail> list =
        serverCollection.getServers(request.getDataCenter(), request.getCluster());

    int maxCount = Math.max(0, request.getMaxCount());
    if (list.size() > maxCount) {
      Collections.shuffle(list);
      list = list.subList(0, maxCount);
    }

    GetServersResponseMessage response = new GetServersResponseMessage(list);
    HandlerUtil.writeResponseMsg(ctx, response);
  }
}
