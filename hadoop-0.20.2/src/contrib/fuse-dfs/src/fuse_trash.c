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


#include <hdfs.h>
#include <strings.h>

#include "fuse_dfs.h"
#include "fuse_trash.h"
#include "fuse_context_handle.h"


const char *const TrashPrefixDir = "/user/root/.Trash";
const char *const TrashDir = "/user/root/.Trash/Current";

#define TRASH_RENAME_TRIES  100

//
// NOTE: this function is a c implementation of org.apache.hadoop.fs.Trash.moveToTrash(Path path).
//

int move_to_trash(const char *item, hdfsFS userFS) {

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(item);
  assert(dfs);
  assert('/' == *item);
  assert(rindex(item,'/') >= 0);


  char fname[4096]; // or last element of the directory path
  char parent_dir[4096]; // the directory the fname resides in

  if (strlen(item) > sizeof(fname) - strlen(TrashDir)) {
    ERROR("Buffer too small to accomodate path of len %d", (int)strlen(item));
    return -EIO;
  }

  // separate the file name and the parent directory of the item to be deleted
  {
    int length_of_parent_dir = rindex(item, '/') - item ;
    int length_of_fname = strlen(item) - length_of_parent_dir - 1; // the '/'

    // note - the below strncpys should be safe from overflow because of the check on item's string length above.
    strncpy(parent_dir, item, length_of_parent_dir);
    parent_dir[length_of_parent_dir ] = 0;
    strncpy(fname, item + length_of_parent_dir + 1, strlen(item));
    fname[length_of_fname + 1] = 0;
  }

  // create the target trash directory
  char trash_dir[4096];
  if (snprintf(trash_dir, sizeof(trash_dir), "%s%s", TrashDir, parent_dir) 
      >= sizeof trash_dir) {
    ERROR("Move to trash error target not big enough for %s", item);
    return -EIO;
  }

  // create the target trash directory in trash (if needed)
  if ( hdfsExists(userFS, trash_dir)) {
    // make the directory to put it in in the Trash - NOTE
    // hdfsCreateDirectory also creates parents, so Current will be created if it does not exist.
    if (hdfsCreateDirectory(userFS, trash_dir)) {
      return -EIO;
    }
  }

  //
  // if the target path in Trash already exists, then append with
  // a number. Start from 1.
  //
  char target[4096];
  int j ;
  if ( snprintf(target, sizeof target,"%s/%s",trash_dir, fname) >= sizeof target) {
    ERROR("Move to trash error target not big enough for %s", item);
    return -EIO;
  }

  // NOTE: this loop differs from the java version by capping the #of tries
  for (j = 1; ! hdfsExists(userFS, target) && j < TRASH_RENAME_TRIES ; j++) {
    if (snprintf(target, sizeof target,"%s/%s.%d",trash_dir, fname, j) >= sizeof target) {
      ERROR("Move to trash error target not big enough for %s", item);
      return -EIO;
    }
  }
  if (hdfsRename(userFS, item, target)) {
    ERROR("Trying to rename %s to %s", item, target);
    return -EIO;
  }
  return 0;
} 


int hdfsDeleteWithTrash(hdfsFS userFS, const char *path, int useTrash) {

  // move the file to the trash if this is enabled and its not actually in the trash.
  if (useTrash && strncmp(path, TrashPrefixDir, strlen(TrashPrefixDir)) != 0) {
    int ret= move_to_trash(path, userFS);
    return ret;
  }

  if (hdfsDelete(userFS, path)) {
    ERROR("Trying to delete the file %s", path);
    return -EIO;
  }
  return 0;

}
