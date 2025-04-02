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

package org.apache.hive.service.cli;

import org.apache.hive.service.rpc.thrift.TFetchOrientation;

/**
 * FetchOrientation.
 *
 */
public enum FetchOrientation {
  FETCH_NEXT(TFetchOrientation.FETCH_NEXT),
  FETCH_PRIOR(TFetchOrientation.FETCH_PRIOR),
  FETCH_RELATIVE(TFetchOrientation.FETCH_RELATIVE),
  FETCH_ABSOLUTE(TFetchOrientation.FETCH_ABSOLUTE),
  FETCH_FIRST(TFetchOrientation.FETCH_FIRST),
  FETCH_LAST(TFetchOrientation.FETCH_LAST);

  private TFetchOrientation tFetchOrientation;

  FetchOrientation(TFetchOrientation tFetchOrientation) {
    this.tFetchOrientation = tFetchOrientation;
  }

  public static FetchOrientation getFetchOrientation(TFetchOrientation tFetchOrientation) {
    for (FetchOrientation fetchOrientation : values()) {
      if (tFetchOrientation.equals(fetchOrientation.toTFetchOrientation())) {
        return fetchOrientation;
      }
    }
    // TODO: Should this really default to FETCH_NEXT?
    return FETCH_NEXT;
  }

  public TFetchOrientation toTFetchOrientation() {
    return tFetchOrientation;
  }
}
