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
package org.apache.spark.internal.io.cloud.abortable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.Path;

public class AbstractAbortableFileSystem extends DelegateToFileSystem {

  public AbstractAbortableFileSystem(
      URI theUri,
      Configuration conf) throws IOException, URISyntaxException {
    super(theUri, new AbortableFileSystem(), conf, AbortableFileSystem.ABORTABLE_FS_SCHEME, false);
  }

  @Override
  public boolean hasPathCapability(Path path, String capability) throws IOException {
    if (capability == CommonPathCapabilities.ABORTABLE_STREAM) {
      return true;
    } else {
      return super.hasPathCapability(path, capability);
    }
  }
}
