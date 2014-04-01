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
#include "fuse_users.h"
#include "fuse_impls.h"
#include "fuse_connect.h"

 int dfs_chown(const char *path, uid_t uid, gid_t gid)
{
  TRACE1("chown", path)

  int ret = 0;
  char *user = NULL;
  char *group = NULL;
  hdfsFS userFS = NULL;

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  user = getUsername(uid);
  if (NULL == user) {
    ERROR("Could not lookup the user id string %d",(int)uid); 
    ret = -EIO;
    goto cleanup;
  }

  group = getGroup(gid);
  if (group == NULL) {
    ERROR("Could not lookup the group id string %d",(int)gid);
    ret = -EIO;
    goto cleanup;
  } 

  userFS = doConnectAsUser(dfs->nn_hostname, dfs->nn_port);
  if (userFS == NULL) {
    ERROR("Could not connect to HDFS");
    ret = -EIO;
    goto cleanup;
  }

  if (hdfsChown(userFS, path, user, group)) {
    ERROR("Could not chown %s to %d:%d", path, (int)uid, gid);
    ret = (errno > 0) ? -errno : -EIO;
    goto cleanup;
  }

cleanup:
  if (userFS && doDisconnect(userFS)) {
    ret = -EIO;
  }
  if (user) {
    free(user);
  }
  if (group) {
    free(group);
  }

  return ret;

}
