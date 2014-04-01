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

import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for URL stream handlers.
 * 
 * There is only one handler whose job is to create UrlConnections. A
 * FsUrlConnection relies on FileSystem to choose the appropriate FS
 * implementation.
 * 
 * Before returning our handler, we make sure that FileSystem knows an
 * implementation for the requested scheme/protocol.
 */
public class FsUrlStreamHandlerFactory implements
    URLStreamHandlerFactory {

  // The configuration holds supported FS implementation class names.
  private Configuration conf;

  // This map stores whether a protocol is know or not by FileSystem
  private Map<String, Boolean> protocols = new HashMap<String, Boolean>();

  // The URL Stream handler
  private java.net.URLStreamHandler handler;

  public FsUrlStreamHandlerFactory() {
    this.conf = new Configuration();
    // force the resolution of the configuration files
    // this is required if we want the factory to be able to handle
    // file:// URLs
    this.conf.getClass("fs.file.impl", null);
    this.handler = new FsUrlStreamHandler(this.conf);
  }

  public FsUrlStreamHandlerFactory(Configuration conf) {
    this.conf = new Configuration(conf);
    // force the resolution of the configuration files
    this.conf.getClass("fs.file.impl", null);
    this.handler = new FsUrlStreamHandler(this.conf);
  }

  public java.net.URLStreamHandler createURLStreamHandler(String protocol) {
    if (!protocols.containsKey(protocol)) {
      boolean known =
          (conf.getClass("fs." + protocol + ".impl", null) != null);
      protocols.put(protocol, known);
    }
    if (protocols.get(protocol)) {
      return handler;
    } else {
      // FileSystem does not know the protocol, let the VM handle this
      return null;
    }
  }

}
