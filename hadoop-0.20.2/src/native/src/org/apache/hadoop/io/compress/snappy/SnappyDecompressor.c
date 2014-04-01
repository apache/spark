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

#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HADOOP_SNAPPY_LIBRARY

#if defined HAVE_STDIO_H
  #include <stdio.h>
#else
  #error 'stdio.h not found'
#endif

#if defined HAVE_STDLIB_H
  #include <stdlib.h>
#else
  #error 'stdlib.h not found'
#endif

#if defined HAVE_STRING_H
  #include <string.h>
#else
  #error 'string.h not found'
#endif

#if defined HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #error 'dlfcn.h not found'
#endif

#include "org_apache_hadoop_io_compress_snappy.h"
#include "org_apache_hadoop_io_compress_snappy_SnappyDecompressor.h"

static jfieldID SnappyDecompressor_clazz;
static jfieldID SnappyDecompressor_compressedDirectBuf;
static jfieldID SnappyDecompressor_compressedDirectBufLen;
static jfieldID SnappyDecompressor_uncompressedDirectBuf;
static jfieldID SnappyDecompressor_directBufferSize;

static snappy_status (*dlsym_snappy_uncompress)(const char*, size_t, char*, size_t*);

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyDecompressor_initIDs
(JNIEnv *env, jclass clazz){

  // Load libsnappy.so
  void *libsnappy = dlopen(HADOOP_SNAPPY_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libsnappy) {
    char* msg = (char*)malloc(1000);
    snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_SNAPPY_LIBRARY, dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return;
  }

  // Locate the requisite symbols from libsnappy.so
  dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_snappy_uncompress, env, libsnappy, "snappy_uncompress");

  SnappyDecompressor_clazz = (*env)->GetStaticFieldID(env, clazz, "clazz",
                                                   "Ljava/lang/Class;");
  SnappyDecompressor_compressedDirectBuf = (*env)->GetFieldID(env,clazz,
                                                           "compressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  SnappyDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env,clazz,
                                                              "compressedDirectBufLen", "I");
  SnappyDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env,clazz,
                                                             "uncompressedDirectBuf",
                                                             "Ljava/nio/Buffer;");
  SnappyDecompressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                         "directBufferSize", "I");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyDecompressor_decompressBytesDirect
(JNIEnv *env, jobject thisj){
  // Get members of SnappyDecompressor
  jobject clazz = (*env)->GetStaticObjectField(env,thisj, SnappyDecompressor_clazz);
  jobject compressed_direct_buf = (*env)->GetObjectField(env,thisj, SnappyDecompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env,thisj, SnappyDecompressor_compressedDirectBufLen);
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env,thisj, SnappyDecompressor_uncompressedDirectBuf);
  size_t uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, SnappyDecompressor_directBufferSize);

  // Get the input direct buffer
  LOCK_CLASS(env, clazz, "SnappyDecompressor");
  const char* compressed_bytes = (const char*)(*env)->GetDirectBufferAddress(env, compressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyDecompressor");

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  LOCK_CLASS(env, clazz, "SnappyDecompressor");
  char* uncompressed_bytes = (char *)(*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyDecompressor");

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  snappy_status ret = dlsym_snappy_uncompress(compressed_bytes, compressed_direct_buf_len, uncompressed_bytes, &uncompressed_direct_buf_len);
  if (ret == SNAPPY_BUFFER_TOO_SMALL){
    THROW(env, "Ljava/lang/InternalError", "Could not decompress data. Buffer length is too small.");
  } else if (ret == SNAPPY_INVALID_INPUT){
    THROW(env, "Ljava/lang/InternalError", "Could not decompress data. Input is invalid.");
  } else if (ret != SNAPPY_OK){
    THROW(env, "Ljava/lang/InternalError", "Could not decompress data.");
  }

  (*env)->SetIntField(env, thisj, SnappyDecompressor_compressedDirectBufLen, 0);

  return (jint)uncompressed_direct_buf_len;
}

#endif //define HADOOP_SNAPPY_LIBRARY
