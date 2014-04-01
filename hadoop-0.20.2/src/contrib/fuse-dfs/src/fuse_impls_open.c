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

#include "fuse_dfs.h"
#include "fuse_impls.h"
#include "fuse_connect.h"
#include "fuse_file_handle.h"

int dfs_open(const char *path, struct fuse_file_info *fi)
{
  TRACE1("open", path)

  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert('/' == *path);
  assert(dfs);

  int ret = 0;

  // 0x8000 is always passed in and hadoop doesn't like it, so killing it here
  // bugbug figure out what this flag is and report problem to Hadoop JIRA
  int flags = (fi->flags & 0x7FFF);

  // retrieve dfs specific data
  dfs_fh *fh = (dfs_fh*)calloc(1, sizeof (dfs_fh));
  if (fh == NULL) {
    ERROR("Malloc of new file handle failed");
    return -EIO;
  }

  fh->fs = doConnectAsUser(dfs->nn_hostname, dfs->nn_port);
  if (fh->fs == NULL) {
    ERROR("Could not connect to dfs");
    return -EIO;
  }

  if (flags & O_RDWR) {
    hdfsFileInfo *info = hdfsGetPathInfo(fh->fs,path);
    if (info == NULL) {
      // File does not exist (maybe?); interpret it as a O_WRONLY
      // If the actual error was something else, we'll get it again when
      // we try to open the file.
      flags ^= O_RDWR;
      flags |= O_WRONLY;
    } else {
      // File exists; open this as read only.
      flags ^= O_RDWR;
      flags |= O_RDONLY;
    }
  }

  if ((fh->hdfsFH = hdfsOpenFile(fh->fs, path, flags,  0, 0, 0)) == NULL) {
    ERROR("Could not open file %s (errno=%d)", path, errno);
    if (errno == 0 || errno == EINTERNAL) {
      return -EIO;
    }
    return -errno;
  }

  pthread_mutex_init(&fh->mutex, NULL);

  if (fi->flags & O_WRONLY || fi->flags & O_CREAT) {
    fh->buf = NULL;
  } else  {
    assert(dfs->rdbuffer_size > 0);
    fh->buf = (char*)malloc(dfs->rdbuffer_size * sizeof(char));
    if (NULL == fh->buf) {
      ERROR("Could not allocate memory for a read for file %s\n", path);
      ret = -EIO;
    }
    fh->buffersStartOffset = 0;
    fh->bufferSize = 0;
  }
  fi->fh = (uint64_t)fh;

  return ret;
}
