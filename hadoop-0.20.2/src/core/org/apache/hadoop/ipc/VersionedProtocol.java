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

package org.apache.hadoop.ipc;

import java.io.IOException;

/**
 * Superclass of all protocols that use Hadoop RPC.
 * Subclasses of this interface are also supposed to have
 * a static final long versionID field.
 */
public interface VersionedProtocol {
  
  /**
   * Return protocol version corresponding to protocol interface.
   * @param protocol The classname of the protocol interface
   * @param clientVersion The version of the protocol that the client speaks
   * @return the version that the server will speak
   */
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException;
}
