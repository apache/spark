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

#ifndef __FUSE_FILE_HANDLE_H__
#define __FUSE_FILE_HANDLE_H__

#include <hdfs.h>
#include <pthread.h>

/**
 *
 * dfs_fh_struct is passed around for open files. Fuse provides a hook (the context) 
 * for storing file specific data.
 *
 * 2 Types of information:
 * a) a read buffer for performance reasons since fuse is typically called on 4K chunks only
 * b) the hdfs fs handle 
 *
 */
typedef struct dfs_fh_struct {
  hdfsFile hdfsFH;
  char *buf;
  tSize bufferSize;  //what is the size of the buffer we have
  off_t buffersStartOffset; //where the buffer starts in the file
  hdfsFS fs; // for reads/writes need to access as the real user
  pthread_mutex_t mutex;
} dfs_fh;

#endif
