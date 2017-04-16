/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
 
#include <jni.h>
#include "file_descriptor.h"
#include "org_apache_hadoop.h"

// class of java.io.FileDescriptor
static jclass fd_class;
// the internal field for the integer fd
static jfieldID fd_descriptor;
// the no-argument constructor
static jmethodID fd_constructor;


void fd_init(JNIEnv* env)
{
  if (fd_class != NULL) return; // already initted

  fd_class = (*env)->FindClass(env, "java/io/FileDescriptor");
  PASS_EXCEPTIONS(env);
  fd_class = (*env)->NewGlobalRef(env, fd_class);

  fd_descriptor = (*env)->GetFieldID(env, fd_class, "fd", "I");
  PASS_EXCEPTIONS(env);
  fd_constructor = (*env)->GetMethodID(env, fd_class, "<init>", "()V");
}

void fd_deinit(JNIEnv *env) {
  if (fd_class != NULL) {
    (*env)->DeleteGlobalRef(env, fd_class);
    fd_class = NULL;
  }
  fd_descriptor = NULL;
  fd_constructor = NULL;
}

/*
 * Given an instance 'obj' of java.io.FileDescriptor, return the
 * underlying fd, or throw if unavailable
 */
int fd_get(JNIEnv* env, jobject obj) {
  if (obj == NULL) {
    THROW(env, "java/lang/NullPointerException",
          "FileDescriptor object is null");
    return -1;
  }
  return (*env)->GetIntField(env, obj, fd_descriptor);
}

/*
 * Create a FileDescriptor object corresponding to the given int fd
 */
jobject fd_create(JNIEnv *env, int fd) {
  jobject obj = (*env)->NewObject(env, fd_class, fd_constructor);
  PASS_EXCEPTIONS_RET(env, NULL);

  (*env)->SetIntField(env, obj, fd_descriptor, fd);
  return obj;
} 
