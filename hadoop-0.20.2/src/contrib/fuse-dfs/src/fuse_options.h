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

#ifndef __FUSE_OPTIONS_H__
#define __FUSE_OPTIONS_H__

/** options for fuse_opt.h */
struct options {
  char* protected;
  char* server;
  int port;
  int debug;
  int read_only;
  int initchecks;
  int no_permissions;
  int usetrash;
  int entry_timeout;
  int attribute_timeout;
  int private;
  size_t rdbuffer_size;
  int direct_io;
} options;

extern struct fuse_opt dfs_opts[];
void print_options();
void print_usage(const char *pname);
int dfs_options(void *data, const char *arg, int key,  struct fuse_args *outargs);

#endif
