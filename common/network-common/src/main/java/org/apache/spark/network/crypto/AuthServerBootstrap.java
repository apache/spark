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

package org.apache.spark.network.crypto;

import io.netty.channel.Channel;

import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.TransportConf;

/**
 * A bootstrap which is executed on a TransportServer's client channel once a client connects
 * to the server, enabling authentication using Spark's auth protocol (and optionally SASL for
 * clients that don't support the new protocol).
 *
 * It also automatically falls back to SASL if the new encryption backend is disabled, so that
 * callers only need to install this bootstrap when authentication is enabled.
 */
public class AuthServerBootstrap implements TransportServerBootstrap {

  private final TransportConf conf;
  private final SecretKeyHolder secretKeyHolder;

  public AuthServerBootstrap(TransportConf conf, SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    this.secretKeyHolder = secretKeyHolder;
  }

  @Override
  public RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler) {
    if (!conf.encryptionEnabled()) {
      TransportServerBootstrap sasl = new SaslServerBootstrap(conf, secretKeyHolder);
      return sasl.doBootstrap(channel, rpcHandler);
    }

    return new AuthRpcHandler(conf, channel, rpcHandler, secretKeyHolder);
  }

}
