/*
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
 #include <assert.h>
 #include <errno.h>
 #include <jni.h>

#include "org_apache_hadoop.h"

typedef struct errno_mapping {
  int errno_val;
  char *errno_str;
} errno_mapping_t;

// Macro to define structs like {FOO, "FOO"} for each errno value
#define MAPPING(x) {x, #x}
static errno_mapping_t ERRNO_MAPPINGS[] = {
  MAPPING(EPERM),
  MAPPING(ENOENT),
  MAPPING(ESRCH),
  MAPPING(EINTR),
  MAPPING(EIO),
  MAPPING(ENXIO),
  MAPPING(E2BIG),
  MAPPING(ENOEXEC),
  MAPPING(EBADF),
  MAPPING(ECHILD),
  MAPPING(EAGAIN),
  MAPPING(ENOMEM),
  MAPPING(EACCES),
  MAPPING(EFAULT),
  MAPPING(ENOTBLK),
  MAPPING(EBUSY),
  MAPPING(EEXIST),
  MAPPING(EXDEV),
  MAPPING(ENODEV),
  MAPPING(ENOTDIR),
  MAPPING(EISDIR),
  MAPPING(EINVAL),
  MAPPING(ENFILE),
  MAPPING(EMFILE),
  MAPPING(ENOTTY),
  MAPPING(ETXTBSY),
  MAPPING(EFBIG),
  MAPPING(ENOSPC),
  MAPPING(ESPIPE),
  MAPPING(EROFS),
  MAPPING(EMLINK),
  MAPPING(EPIPE),
  MAPPING(EDOM),
  MAPPING(ERANGE),
  {-1, NULL}
};

static jclass enum_class;
static jmethodID enum_valueOf;
static jclass errno_class;

void errno_enum_init(JNIEnv *env) {
  if (enum_class != NULL) return;

  enum_class = (*env)->FindClass(env, "java/lang/Enum");
  PASS_EXCEPTIONS(env);
  enum_class = (*env)->NewGlobalRef(env, enum_class);
  enum_valueOf = (*env)->GetStaticMethodID(env, enum_class,
    "valueOf", "(Ljava/lang/Class;Ljava/lang/String;)Ljava/lang/Enum;");
  PASS_EXCEPTIONS(env);

  errno_class = (*env)->FindClass(env, "org/apache/hadoop/io/nativeio/Errno");
  PASS_EXCEPTIONS(env);
  errno_class = (*env)->NewGlobalRef(env, errno_class);
}

void errno_enum_deinit(JNIEnv *env) {
  if (enum_class != NULL) {
    (*env)->DeleteGlobalRef(env, enum_class);
    enum_class = NULL;
  }
  if (errno_class != NULL) {
    (*env)->DeleteGlobalRef(env, errno_class);
    errno_class = NULL;
  }
  enum_valueOf = NULL;
}


static char *errno_to_string(int errnum) {
  int i;
  for (i = 0; ERRNO_MAPPINGS[i].errno_str != NULL; i++) {
    if (ERRNO_MAPPINGS[i].errno_val == errnum)
      return ERRNO_MAPPINGS[i].errno_str;
  }
  return "UNKNOWN";
}

jobject errno_to_enum(JNIEnv *env, int errnum) {
  char *str = errno_to_string(errnum);
  assert(str != NULL);

  jstring jstr = (*env)->NewStringUTF(env, str);
  PASS_EXCEPTIONS_RET(env, NULL);

  return (*env)->CallStaticObjectMethod(
    env, enum_class, enum_valueOf, errno_class, jstr);
}
