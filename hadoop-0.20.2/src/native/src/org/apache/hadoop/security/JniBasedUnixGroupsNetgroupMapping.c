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

#include <netdb.h>

#include "org_apache_hadoop_security_JniBasedUnixGroupsNetgroupMapping.h"
#include "org_apache_hadoop.h"

struct listElement {
   char * string;
   struct listElement * next;
};

typedef struct listElement UserList;

JNIEXPORT jobjectArray JNICALL 
Java_org_apache_hadoop_security_JniBasedUnixGroupsNetgroupMapping_getUsersForNetgroupJNI
(JNIEnv *env, jobject jobj, jstring jgroup) {

  // pointers to free at the end
  const char *cgroup  = NULL;
  jobjectArray jusers = NULL;

  // do we need to end the group lookup?
  int setnetgrentCalledFlag = 0;

  // if not NULL then THROW exception
  char *errorMessage = NULL;

  cgroup = (*env)->GetStringUTFChars(env, jgroup, NULL);
  if (cgroup == NULL) {
    goto END;
  }

  //--------------------------------------------------
  // get users
  // see man pages for setnetgrent, getnetgrent and endnetgrent

  UserList *userListHead = NULL;
  int       userListSize = 0;

  // set the name of the group for subsequent calls to getnetgrent
  // note that we want to end group lokup regardless whether setnetgrent
  // was successfull or not (as long as it was called we need to call
  // endnetgrent)
  setnetgrentCalledFlag = 1;
  if(setnetgrent(cgroup) == 1) {
    UserList *current = NULL;
    // three pointers are for host, user, domain, we only care
    // about user now
    char *p[3];
    while(getnetgrent(p, p + 1, p + 2)) {
      if(p[1]) {
        current = (UserList *)malloc(sizeof(UserList));
        current->string = malloc(strlen(p[1]) + 1);
        strcpy(current->string, p[1]);
        current->next = userListHead;
        userListHead = current;
        userListSize++;
      }
    }
  }

  //--------------------------------------------------
  // build return data (java array)

  jusers = (jobjectArray)(*env)->NewObjectArray(env,
    userListSize, 
    (*env)->FindClass(env, "java/lang/String"),
    NULL);
  if (jusers == NULL) {
    errorMessage = "java/lang/OutOfMemoryError";
    goto END;
  }

  UserList * current = NULL;

  // note that the loop iterates over list but also over array (i)
  int i = 0;
  for(current = userListHead; current != NULL; current = current->next) {
    jstring juser = (*env)->NewStringUTF(env, current->string);
    if (juser == NULL) {
      errorMessage = "java/lang/OutOfMemoryError";
      goto END;
    }
    (*env)->SetObjectArrayElement(env, jusers, i++, juser);
  }


END:

  // cleanup
  if(cgroup) { (*env)->ReleaseStringUTFChars(env, jgroup, cgroup); }
  if(setnetgrentCalledFlag) { endnetgrent(); }
  while(userListHead) {
    UserList *current = userListHead;
    userListHead = userListHead->next;
    if(current->string) { free(current->string); }
    free(current);
  }

  // return results or THROW
  if(errorMessage) {
    THROW(env, errorMessage, NULL);
    return NULL;
  } else {
    return jusers;
  }
}
