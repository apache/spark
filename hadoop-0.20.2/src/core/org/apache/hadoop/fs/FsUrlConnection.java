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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;

/**
 * Representation of a URL connection to open InputStreams.
 */
class FsUrlConnection extends URLConnection {

  private Configuration conf;

  private InputStream is;

  FsUrlConnection(Configuration conf, URL url) {
    super(url);
    this.conf = conf;
  }

  @Override
  public void connect() throws IOException {
    try {
      FileSystem fs = FileSystem.get(url.toURI(), conf);
      is = fs.open(new Path(url.getPath()));
    } catch (URISyntaxException e) {
      throw new IOException(e.toString());
    }
  }

  /* @inheritDoc */
  @Override
  public InputStream getInputStream() throws IOException {
    if (is == null) {
      connect();
    }
    return is;
  }

}
