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

#include <math.h>
#include <pthread.h>
#include <grp.h>
#include <pwd.h>

#include "fuse_dfs.h"
#include "fuse_stat_struct.h"
#include "fuse_context_handle.h"

/*
 * getpwuid and getgrgid return static structs so we safeguard the contents
 * while retrieving fields using the 2 structs below.
 * NOTE: if using both, always get the passwd struct firt!
 */
extern pthread_mutex_t passwdstruct_mutex; 
extern pthread_mutex_t groupstruct_mutex;

const int default_id = 99; // nobody  - not configurable since soon uids in dfs, yeah!
const int blksize = 512;

/**
 * Converts from a hdfs hdfsFileInfo to a POSIX stat struct
 *
 */
int fill_stat_structure(hdfsFileInfo *info, struct stat *st) 
{
  assert(st);
  assert(info);

  // initialize the stat structure
  memset(st, 0, sizeof(struct stat));

  // by default: set to 0 to indicate not supported for directory because we cannot (efficiently) get this info for every subdirectory
  st->st_nlink = (info->mKind == kObjectKindDirectory) ? 0 : 1;

  uid_t owner_id = default_id;
  if (info->mOwner != NULL) {
    //
    // Critical section - protect from concurrent calls in different threads since
    // the struct below is static.
    // (no returns until end)
    //
    pthread_mutex_lock(&passwdstruct_mutex);

    struct passwd *passwd_info = getpwnam(info->mOwner);
    owner_id = passwd_info == NULL ? default_id : passwd_info->pw_uid;

    //
    // End critical section 
    // 
    pthread_mutex_unlock(&passwdstruct_mutex);

  } 

  gid_t group_id = default_id;

  if (info->mGroup != NULL) {
    //
    // Critical section - protect from concurrent calls in different threads since
    // the struct below is static.
    // (no returns until end)
    //
    pthread_mutex_lock(&groupstruct_mutex);

    struct group *grp = getgrnam(info->mGroup);
    group_id = grp == NULL ? default_id : grp->gr_gid;

    //
    // End critical section 
    // 
    pthread_mutex_unlock(&groupstruct_mutex);

  }

  short perm = (info->mKind == kObjectKindDirectory) ? (S_IFDIR | 0777) :  (S_IFREG | 0666);
  if (info->mPermissions > 0) {
    perm = (info->mKind == kObjectKindDirectory) ? S_IFDIR:  S_IFREG ;
    perm |= info->mPermissions;
  }

  // set stat metadata
  st->st_size     = (info->mKind == kObjectKindDirectory) ? 4096 : info->mSize;
  st->st_blksize  = blksize;
  st->st_blocks   =  ceil(st->st_size/st->st_blksize);
  st->st_mode     = perm;
  st->st_uid      = owner_id;
  st->st_gid      = group_id;
  st->st_atime    = info->mLastAccess;
  st->st_mtime    = info->mLastMod;
  st->st_ctime    = info->mLastMod;

  return 0;
}

