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
#include "fuse_file_handle.h"
#include "fuse_connect.h"

/**
 * This mutex is to protect releasing a file handle in case the user calls close in different threads
 * and fuse passes these calls to here.
 */
pthread_mutex_t release_mutex = PTHREAD_MUTEX_INITIALIZER;

int dfs_release (const char *path, struct fuse_file_info *fi) {
  TRACE1("release", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  int ret = 0;

  //
  // Critical section - protect from multiple close calls in different threads.
  // (no returns until end)
  //

  pthread_mutex_lock(&release_mutex);

  if (NULL != (void*)fi->fh) {

    dfs_fh *fh = (dfs_fh*)fi->fh;
    assert(fh);

    hdfsFile file_handle = (hdfsFile)fh->hdfsFH;

    if (NULL != file_handle) {
      if (hdfsCloseFile(fh->fs, file_handle) != 0) {
        ERROR("Could not close handle %ld for %s\n",(long)file_handle, path);
        ret = -EIO;
      }
    }

    if (fh->buf != NULL) {
      free(fh->buf);
    }

    if (doDisconnect(fh->fs)) {
      ret = -EIO;
    }

    // this is always created and initialized, so always destroy it. (see dfs_open)
    pthread_mutex_destroy(&fh->mutex);

    free(fh);

    fi->fh = (uint64_t)0;
  }

  pthread_mutex_unlock(&release_mutex);

  //
  // End critical section 
  // 

  return ret;
}
