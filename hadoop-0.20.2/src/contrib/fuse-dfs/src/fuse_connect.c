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

#include "hdfs.h"
#include "fuse_dfs.h"
#include "fuse_connect.h"
#include "fuse_users.h" 

#include <search.h>

#define MAX_ELEMENTS (16 * 1024)
static struct hsearch_data *fsTable = NULL;
static pthread_mutex_t tableMutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Allocate a hash table for fs handles. Returns 0 on success,
 * -1 on failure.
 */
int allocFsTable(void) {
  assert(NULL == fsTable);
  fsTable = calloc(1, sizeof(struct hsearch_data));
  if (0 == hcreate_r(MAX_ELEMENTS, fsTable)) {
    ERROR("Unable to initialize connection table");
    return -1;
  }
  return 0;
}

/*
 * Find a fs handle for the given key. Returns a fs handle, 
 * or NULL if there is no fs for the given key.
 */
static hdfsFS findFs(char *key) {
  ENTRY entry;
  ENTRY *entryP = NULL;
  entry.key = key;
  if (0 == hsearch_r(entry, FIND, &entryP, fsTable)) {
    return NULL;
  }
  assert(NULL != entryP->data);
  return (hdfsFS)entryP->data;
}

/*
 * Insert the given fs handle into the table.
 * Returns 0 on success, -1 on failure.
 */
static int insertFs(char *key, hdfsFS fs) {
  ENTRY entry;
  ENTRY *entryP = NULL;
  assert(NULL != fs);
  entry.key = strdup(key);
  if (entry.key == NULL) {
    return -1;
  }
  entry.data = (void*)fs;
  if (0 == hsearch_r(entry, ENTER, &entryP, fsTable)) {
    return -1;
  }
  return 0;
}

/*
 * Connect to the NN as the current user/group.
 * Returns a fs handle on success, or NULL on failure.
 */
hdfsFS doConnectAsUser(const char *hostname, int port) {
  uid_t uid = fuse_get_context()->uid;
  char *user = getUsername(uid);
  int ret;
  hdfsFS fs = NULL;
  if (NULL == user) {
    goto done;
  }

  ret = pthread_mutex_lock(&tableMutex);
  assert(0 == ret);

  fs = findFs(user);
  if (NULL == fs) {
    fs = hdfsConnectAsUserNewInstance(hostname, port, user);
    if (NULL == fs) {
      ERROR("Unable to create fs for user %s", user);
      goto done;
    }
    if (-1 == insertFs(user, fs)) {
      ERROR("Unable to cache fs for user %s", user);
    }
  }

done:
  ret = pthread_mutex_unlock(&tableMutex);
  assert(0 == ret);
  if (user) {
    free(user);
  }
  return fs;
}

/*
 * We currently cache a fs handle per-user in this module rather
 * than use the FileSystem cache in the java client. Therefore
 * we do not disconnect the fs handle here.
 */
int doDisconnect(hdfsFS fs) {
  return 0;
}
