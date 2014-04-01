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

#ifndef __FUSE_DFS_H__
#define __FUSE_DFS_H__

#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <strings.h>
#include <syslog.h>

#include <fuse.h>
#include <fuse/fuse_opt.h>

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

//
// Check if a path is in the mount option supplied protected paths.
//
int is_protected(const char *path);

#undef INFO
#define INFO(_fmt, ...) {                       \
  fprintf(stdout, "INFO %s:%d " _fmt "\n",      \
          __FILE__, __LINE__, ## __VA_ARGS__);  \
  syslog(LOG_INFO, "INFO %s:%d " _fmt "\n",     \
          __FILE__, __LINE__, ## __VA_ARGS__);  \
}

#undef DEBUG
#define DEBUG(_fmt, ...) {                      \
  fprintf(stdout, "DEBUG %s:%d " _fmt "\n",     \
          __FILE__, __LINE__, ## __VA_ARGS__);  \
  syslog(LOG_DEBUG, "DEBUG %s:%d " _fmt "\n",   \
          __FILE__, __LINE__, ## __VA_ARGS__);  \
}

#undef ERROR
#define ERROR(_fmt, ...) {                      \
  fprintf(stderr, "ERROR %s:%d " _fmt "\n",     \
          __FILE__, __LINE__, ## __VA_ARGS__);  \
  syslog(LOG_ERR, "ERROR %s:%d " _fmt "\n",     \
          __FILE__, __LINE__, ## __VA_ARGS__);  \
}

//#define DOTRACE
#ifdef DOTRACE
#define TRACE(x) {        \
    DEBUG("TRACE %s", x); \
}

#define TRACE1(x,y) {             \
    DEBUG("TRACE %s %s\n", x, y); \
}
#else
#define TRACE(x) ; 
#define TRACE1(x,y) ; 
#endif

#endif // __FUSE_DFS_H__
