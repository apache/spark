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
#include <jni.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <pwd.h>
#include <string.h>

#include "org_apache_hadoop_security_JniBasedUnixGroupsMapping.h"
#include "org_apache_hadoop.h"

#define CHECK_ERROR_AND_RETURN() \
{\
  if (error == ENOMEM) {\
    THROW(env, "java/lang/OutOfMemoryError", NULL);\
  }\
  if (error == ENOENT) {\
    THROW(env, "java/io/IOException", "No entry for user");\
  }\
  freeObjs(grpBuf, groups, env, juser, cuser);\
  return NULL;\
}

JNIEXPORT jobjectArray JNICALL 
Java_org_apache_hadoop_security_JniBasedUnixGroupsMapping_getGroupForUser 
(JNIEnv *env, jobject jobj, jstring juser) {
  void freeObjs(char *grpBuf, gid_t *groups, JNIEnv *env, jstring juser, 
                const char *cuser);
  extern int getGroupIDList(const char *user, int *ngroups, gid_t **groups);
  extern int getGroupDetails(gid_t group, char **grpBuf);

  char *grpBuf = NULL;
  const char *cuser = (*env)->GetStringUTFChars(env, juser, NULL);
  if (cuser == NULL) {
    return NULL;
  }

  /*Get the number of the groups, and their IDs, this user belongs to*/
  gid_t *groups = NULL;
  int ngroups = 0;
  int error = getGroupIDList(cuser, &ngroups, &groups);
  if (error != 0) {
    CHECK_ERROR_AND_RETURN();
  }

  jobjectArray jgroups = (jobjectArray)(*env)->NewObjectArray(env, ngroups, 
            (*env)->FindClass(env, "java/lang/String"), NULL);
  if (jgroups == NULL) {
    freeObjs(grpBuf, groups, env, juser, cuser);
    THROW(env, "java/lang/OutOfMemoryError", NULL);
    return NULL;
  }

  /*Iterate over the groupIDs and get the group structure for each*/
  int i = 0;
  for (i = 0; i < ngroups; i++) {
    error = getGroupDetails(groups[i],&grpBuf);
    if (error != 0) {
      CHECK_ERROR_AND_RETURN();
    }
    jstring jgrp = (*env)->NewStringUTF(env, ((struct group*)grpBuf)->gr_name);
    if (jgrp == NULL) {
      freeObjs(grpBuf, groups, env, juser, cuser);
      THROW(env, "java/lang/OutOfMemoryError", NULL);
      return NULL;
    }
    (*env)->SetObjectArrayElement(env, jgroups,i,jgrp);
    free(grpBuf);
    grpBuf = NULL;
  }

  freeObjs(grpBuf, groups, env, juser, cuser);
  return jgroups;
}

void freeObjs(char *grpBuf, gid_t *groups, JNIEnv *env, jstring juser, const char *cuser ) {
  if (groups != NULL) {
    free(groups);
    groups = NULL;
  }
  if (grpBuf != NULL) {
    free(grpBuf);
    grpBuf = NULL;
  }
  (*env)->ReleaseStringUTFChars(env, juser, cuser);
}
