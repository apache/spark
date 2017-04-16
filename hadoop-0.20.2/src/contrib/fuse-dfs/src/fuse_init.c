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

#include <strings.h>

#include "fuse_dfs.h"
#include "fuse_init.h"
#include "fuse_options.h"
#include "fuse_context_handle.h"
#include "fuse_connect.h"

// Hacked up function to basically do:
//  protectedpaths = split(options.protected,':');

void init_protectedpaths(dfs_context *dfs) {

  char *tmp = options.protected;

  // handle degenerate case up front.
  if (tmp == NULL || 0 == *tmp) {
    dfs->protectedpaths = (char**)malloc(sizeof(char*));
    dfs->protectedpaths[0] = NULL;
    return;
  }
  assert(tmp);

  if (options.debug) {
    print_options();
  }

  int i = 0;
  while (tmp && (NULL != (tmp = index(tmp,':')))) {
    tmp++; // pass the ,
    i++;
  }
  i++; // for the last entry
  i++; // for the final NULL
  dfs->protectedpaths = (char**)malloc(sizeof(char*)*i);
  assert(dfs->protectedpaths);
  tmp = options.protected;
  int j  = 0;
  while (NULL != tmp && j < i) {
    int length;
    char *eos = index(tmp,':');
    if (NULL != eos) {
      length = eos - tmp; // length of this value
    } else {
      length = strlen(tmp);
    }
    dfs->protectedpaths[j] = (char*)malloc(sizeof(char)*length+1);
    assert(dfs->protectedpaths[j]);
    strncpy(dfs->protectedpaths[j], tmp, length);
    dfs->protectedpaths[j][length] = '\0';
    if (eos) {
      tmp = eos + 1;
    } else {
      tmp = NULL;
    }
    j++;
  }
  dfs->protectedpaths[j] = NULL;
}


void *dfs_init(void) {
  //
  // Create a private struct of data we will pass to fuse here and which
  // will then be accessible on every call.
  //
  dfs_context *dfs = (dfs_context*)malloc(sizeof(dfs_context));
  if (NULL == dfs) {
    ERROR("FATAL: could not malloc dfs_context");
    exit(1);
  }

  // initialize the context
  dfs->debug                 = options.debug;
  dfs->nn_hostname           = options.server;
  dfs->nn_port               = options.port;
  dfs->read_only             = options.read_only;
  dfs->usetrash              = options.usetrash;
  dfs->protectedpaths        = NULL;
  dfs->rdbuffer_size         = options.rdbuffer_size;
  dfs->direct_io             = options.direct_io;

  INFO("Mounting %s:%d", dfs->nn_hostname, dfs->nn_port);

  init_protectedpaths(dfs);
  assert(dfs->protectedpaths != NULL);

  if (dfs->rdbuffer_size <= 0) {
    DEBUG("dfs->rdbuffersize <= 0 = %ld", dfs->rdbuffer_size);
    dfs->rdbuffer_size = 32768;
  }

  if (0 != allocFsTable()) {
    ERROR("FATAL: could not allocate ");
    exit(1);
  }

  return (void*)dfs;
}


void dfs_destroy(void *ptr)
{
  TRACE("destroy")
}
