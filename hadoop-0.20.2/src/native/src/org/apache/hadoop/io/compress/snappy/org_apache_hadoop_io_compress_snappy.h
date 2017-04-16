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


#if !defined ORG_APACHE_HADOOP_IO_COMPRESS_SNAPPY_SNAPPY_H
#define ORG_APACHE_HADOOP_IO_COMPRESS_SNAPPY_SNAPPY_H


#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HADOOP_SNAPPY_LIBRARY

  #if defined HAVE_STDDEF_H
    #include <stddef.h>
  #else
    #error 'stddef.h not found'
  #endif

  #if defined HAVE_SNAPPY_C_H
    #include <snappy-c.h>
  #else
    #error 'Please install snappy-development packages for your platform.'
  #endif

  #if defined HAVE_DLFCN_H
    #include <dlfcn.h>
  #else
    #error "dlfcn.h not found"
  #endif

  #if defined HAVE_JNI_H
    #include <jni.h>
  #else
    #error 'jni.h not found'
  #endif

  #include "org_apache_hadoop.h"

#endif //define HADOOP_SNAPPY_LIBRARY

#endif //ORG_APACHE_HADOOP_IO_COMPRESS_SNAPPY_SNAPPY_H
