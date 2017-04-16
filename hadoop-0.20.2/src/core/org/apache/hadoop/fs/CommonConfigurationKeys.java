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

/** 
 * This class contains constants for configuration keys used
 * in the common code.
 *
 */

public class CommonConfigurationKeys {
  
  /** See src/core/core-default.xml */
  public static final String  FS_DEFAULT_NAME_KEY = "fs.default.name";
  public static final String  FS_DEFAULT_NAME_DEFAULT = "file:///";

  /** See src/core/core-default.xml */
  public static final String  HADOOP_SECURITY_GROUP_MAPPING =
    "hadoop.security.group.mapping";
  /** See src/core/core-default.xml */
  public static final String  HADOOP_SECURITY_AUTHENTICATION =
    "hadoop.security.authentication";
  /** See src/core/core-default.xml */
  public static final String HADOOP_SECURITY_AUTHORIZATION =
    "hadoop.security.authorization";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN =
    "hadoop.security.instrumentation.requires.admin";
  /** See src/core/core-default.xml */
  public static final String  HADOOP_SECURITY_SERVICE_USER_NAME_KEY = 
    "hadoop.security.service.user.name.key";
  public static final String IPC_SERVER_RPC_READ_THREADS_KEY =
                                        "ipc.server.read.threadpool.size";
  public static final int IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;

  /** Internal buffer size for Snappy compressor/decompressors */
  public static final String IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY =
      "io.compression.codec.snappy.buffersize";

  /** Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY */
  public static final int IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT =
      256 * 1024;

  /** See src/core/core-default.xml */
  public static final String HADOOP_RELAXED_VERSION_CHECK_KEY =
      "hadoop.relaxed.worker.version.check";
  public static final boolean HADOOP_RELAXED_VERSION_CHECK_DEFAULT = true;
}

