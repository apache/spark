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
#include "fuse_stat_struct.h"
#include "fuse_connect.h"

int dfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
  TRACE1("readdir", path)
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  assert(dfs);
  assert(path);
  assert(buf);

  hdfsFS userFS = doConnectAsUser(dfs->nn_hostname, dfs->nn_port);
  if (userFS == NULL) {
    ERROR("Could not connect");
    return -EIO;
  }

  // Read dirents. Calling a variant that just returns the final path
  // component (HDFS-975) would save us from parsing it out below.
  int numEntries = 0;
  hdfsFileInfo *info = hdfsListDirectory(userFS, path, &numEntries);

  int ret = 0;
  // NULL means either the directory doesn't exist or maybe IO error.
  if (NULL == info) {
    ret = (errno > 0) ? -errno : -ENOENT;
    goto cleanup;
  }

  int i ;
  for (i = 0; i < numEntries; i++) {
    if (NULL == info[i].mName) {
      ERROR("Path %s info[%d].mName is NULL", path, i);
      continue;
    }

    struct stat st;
    fill_stat_structure(&info[i], &st);

    // Find the final path component
    const char *str = strrchr(info[i].mName, '/');
    if (NULL == str) {
      ERROR("Invalid URI %s", info[i].mName);
      continue;
    }
    str++;

    // pack this entry into the fuse buffer
    int res = 0;
    if ((res = filler(buf,str,&st,0)) != 0) {
      ERROR("Readdir filler failed: %d\n",res);
    }
  }

  // insert '.' and '..'
  const char *const dots [] = { ".",".."};
  for (i = 0 ; i < 2 ; i++)
    {
      struct stat st;
      memset(&st, 0, sizeof(struct stat));

      // set to 0 to indicate not supported for directory because we cannot (efficiently) get this info for every subdirectory
      st.st_nlink =  0;

      // setup stat size and acl meta data
      st.st_size    = 512;
      st.st_blksize = 512;
      st.st_blocks  =  1;
      st.st_mode    = (S_IFDIR | 0777);
      st.st_uid     = default_id;
      st.st_gid     = default_id;
      // todo fix below times
      st.st_atime   = 0;
      st.st_mtime   = 0;
      st.st_ctime   = 0;

      const char *const str = dots[i];

      // flatten the info using fuse's function into a buffer
      int res = 0;
      if ((res = filler(buf,str,&st,0)) != 0) {
	ERROR("Readdir filler failed: %d\n",res);
      }
    }
  // free the info pointers
  hdfsFreeFileInfo(info,numEntries);

cleanup:
  if (doDisconnect(userFS)) {
    ret = -EIO;
    ERROR("Failed to disconnect %d", errno);
  }
  return ret;
}
