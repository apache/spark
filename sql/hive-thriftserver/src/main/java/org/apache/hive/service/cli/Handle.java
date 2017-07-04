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
package org.apache.hive.service.cli;

import org.apache.hive.service.cli.thrift.THandleIdentifier;




public abstract class Handle {

  private final HandleIdentifier handleId;

  public Handle() {
    handleId = new HandleIdentifier();
  }

  public Handle(HandleIdentifier handleId) {
    this.handleId = handleId;
  }

  public Handle(THandleIdentifier tHandleIdentifier) {
    this.handleId = new HandleIdentifier(tHandleIdentifier);
  }

  public HandleIdentifier getHandleIdentifier() {
    return handleId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((handleId == null) ? 0 : handleId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Handle)) {
      return false;
    }
    Handle other = (Handle) obj;
    if (handleId == null) {
      if (other.handleId != null) {
        return false;
      }
    } else if (!handleId.equals(other.handleId)) {
      return false;
    }
    return true;
  }

  @Override
  public abstract String toString();

}
