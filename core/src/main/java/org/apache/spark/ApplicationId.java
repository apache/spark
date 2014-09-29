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

package org.apache.spark;

import java.io.Serializable;

/**
 * This class represents unique application id for identifying each application
 */
public class ApplicationId implements Serializable {

  private String appId;

  public ApplicationId(String appId) {
    this.appId = appId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ApplicationId)) {
      return false;
    } else if (other == this) {
      return true;
    } else if (appId != null) {
      return appId.equals(((ApplicationId)other).appId);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  @Override
  public String toString() {
    return appId;
  }
}
