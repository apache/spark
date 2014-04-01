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

#ifndef __FUSE_USERS_H__
#define __FUSE_USERS_H__

#include <grp.h>
#include <pwd.h>
#include <pthread.h>

/**
 * Overall Note:
 * 1. all these functions should be thread safe.
 * 2. the ones that return char * or char **, generally require
 * the caller to free the return value.
 *
 */


/**
 * Utility for getting the user making the fuse call in char * form
 * NOTE: if non-null return, the return must be freed by the caller.
 */
char *getUsername(uid_t uid);


/**
 * Cleans up a char ** group pointer
 */
void freeGroups(char **groups, int numgroups);

/**
 * Lookup single group. Caller responsible for free of the return value
 */
char *getGroup(gid_t gid);

/**
 * Utility for getting the group from the uid
 * NOTE: if non-null return, the return must be freed by the caller.
 */
char *getGroupUid(uid_t uid) ;


/**
 * lookup the gid based on the uid
 */

gid_t getGidUid(uid_t uid);

/**
 * Utility for getting the groups for the user making the fuse call in char * form
 */
char ** getGroups(uid_t uid, int *num_groups);

#endif
