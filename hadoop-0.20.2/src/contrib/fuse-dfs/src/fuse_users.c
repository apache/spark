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


#include <pthread.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>

#include "fuse_dfs.h"

/*
 * getpwuid and getgrgid return static structs so we safeguard the contents
 * while retrieving fields using the 2 structs below.
 * NOTE: if using both, always get the passwd struct firt!
 */
pthread_mutex_t passwdstruct_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t groupstruct_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Utility for getting the user making the fuse call in char * form
 * NOTE: if non-null return, the return must be freed by the caller.
 */
char *getUsername(uid_t uid) {
  //
  // Critical section - protect from concurrent calls in different threads.
  // since the struct below is static.
  // (no returns until end)
  //

  pthread_mutex_lock(&passwdstruct_mutex);

  struct passwd *userinfo = getpwuid(uid);
  char * ret = userinfo && userinfo->pw_name ? strdup(userinfo->pw_name) : NULL;

  pthread_mutex_unlock(&passwdstruct_mutex);

  //
  // End critical section 
  // 
  return ret;
}

/**
 * Cleans up a char ** group pointer
 */

void freeGroups(char **groups, int numgroups) {
  if (groups == NULL) {
    return;
  }
  int i ;
  for (i = 0; i < numgroups; i++) {
    free(groups[i]);
  }
  free(groups);
}

#define GROUPBUF_SIZE 5

char *getGroup(gid_t gid) {
  //
  // Critical section - protect from concurrent calls in different threads.
  // since the struct below is static.
  // (no returns until end)
  //

  pthread_mutex_lock(&groupstruct_mutex);

  struct group* grp = getgrgid(gid);
  char * ret = grp && grp->gr_name ? strdup(grp->gr_name) : NULL;

  //
  // End critical section 
  // 
  pthread_mutex_unlock(&groupstruct_mutex);

  return ret;
}


/**
 * Utility for getting the group from the uid
 * NOTE: if non-null return, the return must be freed by the caller.
 */
char *getGroupUid(uid_t uid) {
  //
  // Critical section - protect from concurrent calls in different threads
  // since the structs below are static.
  // (no returns until end)
  //

  pthread_mutex_lock(&passwdstruct_mutex);
  pthread_mutex_lock(&groupstruct_mutex);

  char *ret = NULL;
  struct passwd *userinfo = getpwuid(uid);
  if (NULL != userinfo) {
    struct group* grp = getgrgid( userinfo->pw_gid);
    ret = grp && grp->gr_name ? strdup(grp->gr_name) : NULL;
  }

  //
  // End critical section 
  // 
  pthread_mutex_unlock(&groupstruct_mutex);
  pthread_mutex_unlock(&passwdstruct_mutex);

  return ret;
}


/**
 * lookup the gid based on the uid
 */
gid_t getGidUid(uid_t uid) {
  //
  // Critical section - protect from concurrent calls in different threads
  // since the struct below is static.
  // (no returns until end)
  //

  pthread_mutex_lock(&passwdstruct_mutex);

  struct passwd *userinfo = getpwuid(uid);
  gid_t gid = userinfo == NULL ? 0 : userinfo->pw_gid;

  //
  // End critical section 
  // 
  pthread_mutex_unlock(&passwdstruct_mutex);

  return gid;
}

/**
 * Utility for getting the groups for the user making the fuse call in char * form
 */
char ** getGroups(uid_t uid, int *num_groups)
{
  char *user = getUsername(uid);

  if (user == NULL)
    return NULL;

  char **groupnames = NULL;

  // see http://www.openldap.org/lists/openldap-devel/199903/msg00023.html

  //#define GETGROUPS_T 1 
#ifdef GETGROUPS_T
  *num_groups = GROUPBUF_SIZE;

  gid_t* grouplist = malloc(GROUPBUF_SIZE * sizeof(gid_t)); 
  assert(grouplist != NULL);
  gid_t* tmp_grouplist; 
  int rtr;

  gid_t gid = getGidUid(uid);

  if ((rtr = getgrouplist(user, gid, grouplist, num_groups)) == -1) {
    // the buffer we passed in is < *num_groups
    if ((tmp_grouplist = realloc(grouplist, *num_groups * sizeof(gid_t))) != NULL) {
      grouplist = tmp_grouplist;
      getgrouplist(user, gid, grouplist, num_groups);
    }
  }

  groupnames = (char**)malloc(sizeof(char*)* (*num_groups) + 1);
  assert(groupnames);
  int i;
  for (i=0; i < *num_groups; i++)  {
    groupnames[i] = getGroup(grouplist[i]);
    if (groupnames[i] == NULL) {
      ERROR("Could not lookup group %d\n", (int)grouplist[i]);
    }
  } 
  free(grouplist);
  assert(user != NULL);
  groupnames[i] = user;
  *num_groups = *num_groups + 1;
#else

  int i = 0;
  assert(user != NULL);
  groupnames[i] = user;
  i++;

  groupnames[i] = getGroupUid(uid);
  if (groupnames[i]) {
    i++;
  }

  *num_groups = i;

#endif
  return groupnames;
}
