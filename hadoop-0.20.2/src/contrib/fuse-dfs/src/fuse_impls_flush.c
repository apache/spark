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

int dfs_flush(const char *path, struct fuse_file_info *fi) {
  TRACE1("flush", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);
  assert(fi);

  if (NULL == (void*)fi->fh) {
    return  0;
  }

  // note that fuse calls flush on RO files too and hdfs does not like that and will return an error
  if (fi->flags & O_WRONLY) {

    dfs_fh *fh = (dfs_fh*)fi->fh;
    assert(fh);
    hdfsFile file_handle = (hdfsFile)fh->hdfsFH;
    assert(file_handle);

    assert(fh->fs);
    if (hdfsFlush(fh->fs, file_handle) != 0) {
      ERROR("Could not flush %lx for %s\n",(long)file_handle, path);
      return -EIO;
    }
  }

  return 0;
}
